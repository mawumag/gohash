package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	_ "modernc.org/sqlite"
	"net/smtp"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type HashResult struct {
	FilePath string
	Hash     string
}

func main() {
	if len(os.Args) < 3 {
		programName := os.Args[0]
		fmt.Printf("Usage: %s database_path root_directory email", programName)
		return
	}

	databasePath := os.Args[1]
	rootDirectory := os.Args[2]

	files, err := os.ReadDir(rootDirectory)
	SortFileSizeDescend(files)

	if err != nil {
		log.Fatalf("Error reading the specified directory: %v", err)
	}

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatalf("Error closing the database: %v", err)
		}
	}(db)

	// Create the "file_hashes" table if it doesn't exist.
	createTableStmt := `
	CREATE TABLE IF NOT EXISTS file_hashes (
		filename TEXT PRIMARY KEY,
		hash TEXT
	);
	`
	_, err = db.Exec(createTableStmt)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	fileCh := make(chan string)
	hashCh := make(chan HashResult)

	var wg sync.WaitGroup
	numWorkers := 8
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range fileCh {
				// Compute the MD5 hash of the file.
				hash, err := computeFileMD5Hash(filePath)
				if err != nil {
					log.Printf("Error computing MD5 hash for %s: %v", filePath, err)
					continue
				}

				hashCh <- HashResult{FilePath: filePath, Hash: hash}
			}
		}()
	}

	go func() {
		for _, file := range files {
			if !file.IsDir() {
				fileCh <- filepath.Join(rootDirectory, file.Name())
			}
		}
		close(fileCh)

		wg.Wait()
		close(hashCh)
	}()

	var hashError = false
	var hashNew = false
	var hashLogs = ""
	var hashSuccess = 0

	for result := range hashCh {
		var dbHash string
		err = db.QueryRow("SELECT hash FROM file_hashes WHERE filename = ?", result.FilePath).Scan(&dbHash)

		if errors.Is(sql.ErrNoRows, err) {
			// File is not in the database; insert it.
			_, err = db.Exec("INSERT INTO file_hashes (filename, hash) VALUES (?, ?)", result.FilePath, result.Hash)
			if err != nil {
				hashLogs += fmt.Sprintf("Error inserting MD5 hash for %s: %v\n", result.FilePath, err)
				hashError = true
			} else {
				hashLogs += fmt.Sprintf("Inserted MD5 hash for %s: %s\n", result.FilePath, result.Hash)
				hashNew = true
			}
		} else if err != nil {
			hashLogs += fmt.Sprintf("Error querying MD5 hash for %s: %v\n", result.FilePath, err)
			hashError = true
		} else if result.Hash != dbHash {
			hashLogs += fmt.Sprintf("MD5 hash mismatch for %s: stored=%s, computed=%s\n", result.FilePath, dbHash, result.Hash)
			hashError = true
		} else {
			hashSuccess++
			fmt.Printf("MD5 hash match for %s: computed=%s\n", result.FilePath, dbHash)
		}
	}
	hashLogs += fmt.Sprintf("%d files have passed the integrity tests\n", hashSuccess)

	fmt.Print(hashLogs)

	if len(os.Args) > 3 {
		dest := os.Args[3]
		subject := ""

		if hashError {
			subject = "Error detected while verifying integrity"
		} else if hashNew {
			subject = "New files found in the database"
		} else {
			subject = "Integrity check successful"
		}

		sendEmail(dest, subject, hashLogs)
	}
}

func SortFileSizeDescend(files []os.DirEntry) {
	sort.Slice(files, func(i, j int) bool {
		info1, err := files[i].Info()
		if err != nil {
			log.Fatal(err)
		}
		info2, err := files[j].Info()
		if err != nil {
			log.Fatal(err)
		}
		return info1.Size() > info2.Size()
	})
}

func computeFileMD5Hash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Error closing the file: %v", err)
		}
	}(file)

	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", err
	}

	hashBytes := hash.Sum(nil)
	hashStr := hex.EncodeToString(hashBytes)
	return strings.ToLower(hashStr), nil
}

func sendEmail(dest string, subject string, body string) {
	from := From
	password := Password

	to := []string{
		dest,
	}

	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	message := []byte("From: " + from + "\n" +
		"To: " + dest + "\n" +
		"Subject: " + subject + "\n\n" +
		body + "\n")

	auth := smtp.PlainAuth("", from, password, smtpHost)

	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, message)
	if err != nil {
		fmt.Println(err)
		return
	}
}
