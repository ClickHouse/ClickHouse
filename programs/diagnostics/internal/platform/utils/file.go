package utils

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

func FileExists(name string) (bool, error) {
	f, err := os.Stat(name)
	if err == nil {
		if !f.IsDir() {
			return true, nil
		}
		return false, fmt.Errorf("%s is a directory", name)
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func DirExists(name string) (bool, error) {
	f, err := os.Stat(name)
	if err == nil {
		if f.IsDir() {
			return true, nil
		}
		return false, fmt.Errorf("%s is a file", name)
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func CopyFile(sourceFilename string, destFilename string) error {
	exists, err := FileExists(sourceFilename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%s does not exist", sourceFilename)
	}
	source, err := os.Open(sourceFilename)
	if err != nil {
		return err
	}
	defer source.Close()
	destDir := filepath.Dir(destFilename)
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "unable to create directory %s", destDir)
	}

	destination, err := os.Create(destFilename)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}

// patterns passed are an OR - any can be satisfied and the file will be listed

func ListFilesInDirectory(directory string, patterns []string) ([]string, []error) {
	var files []string
	exists, err := DirExists(directory)
	if err != nil {
		return files, []error{err}
	}
	if !exists {
		return files, []error{fmt.Errorf("directory %s does not exist", directory)}
	}
	var pathErrors []error
	_ = filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			pathErrors = append(pathErrors, err)
		} else if !info.IsDir() {
			for _, pattern := range patterns {
				if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
					pathErrors = append(pathErrors, err)
				} else if matched {
					files = append(files, path)
				}
			}
		}
		return nil
	})
	return files, pathErrors
}
