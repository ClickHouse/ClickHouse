package utils_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/stretchr/testify/require"
)

func TestFileExists(t *testing.T) {
	t.Run("returns true for file", func(t *testing.T) {
		tempDir := t.TempDir()
		filepath := path.Join(tempDir, "random.txt")
		_, err := os.Create(filepath)
		require.Nil(t, err)
		exists, err := utils.FileExists(filepath)
		require.True(t, exists)
		require.Nil(t, err)
	})

	t.Run("doesn't return true for not existence file", func(t *testing.T) {
		tempDir := t.TempDir()
		file := path.Join(tempDir, "random.txt")
		exists, err := utils.FileExists(file)
		require.False(t, exists)
		require.Nil(t, err)
	})

	t.Run("doesn't return true for directory", func(t *testing.T) {
		tempDir := t.TempDir()
		exists, err := utils.FileExists(tempDir)
		require.False(t, exists)
		require.NotNil(t, err)
		require.Equal(t, fmt.Sprintf("%s is a directory", tempDir), err.Error())
	})
}

func TestDirExists(t *testing.T) {
	t.Run("doesn't return true for file", func(t *testing.T) {
		tempDir := t.TempDir()
		filepath := path.Join(tempDir, "random.txt")
		_, err := os.Create(filepath)
		require.Nil(t, err)
		exists, err := utils.DirExists(filepath)
		require.False(t, exists)
		require.NotNil(t, err)
		require.Equal(t, fmt.Sprintf("%s is a file", filepath), err.Error())
	})

	t.Run("returns true for directory", func(t *testing.T) {
		tempDir := t.TempDir()
		exists, err := utils.DirExists(tempDir)
		require.True(t, exists)
		require.Nil(t, err)
	})

	t.Run("doesn't return true random directory", func(t *testing.T) {
		exists, err := utils.FileExists(fmt.Sprintf("%d", utils.MakeTimestamp()))
		require.False(t, exists)
		require.Nil(t, err)
	})
}

func TestCopyFile(t *testing.T) {
	t.Run("can copy file", func(t *testing.T) {
		tempDir := t.TempDir()
		sourcePath := path.Join(tempDir, "random.txt")
		_, err := os.Create(sourcePath)
		require.Nil(t, err)
		destPath := path.Join(tempDir, "random-2.txt")
		err = utils.CopyFile(sourcePath, destPath)
		require.Nil(t, err)
	})

	t.Run("can copy nested file", func(t *testing.T) {
		tempDir := t.TempDir()
		sourcePath := path.Join(tempDir, "random.txt")
		_, err := os.Create(sourcePath)
		require.Nil(t, err)
		destPath := path.Join(tempDir, "sub_dir", "random-2.txt")
		err = utils.CopyFile(sourcePath, destPath)
		require.Nil(t, err)
	})

	t.Run("fails when file does not exist", func(t *testing.T) {
		tempDir := t.TempDir()
		sourcePath := path.Join(tempDir, "random.txt")
		destPath := path.Join(tempDir, "random-2.txt")
		err := utils.CopyFile(sourcePath, destPath)
		require.NotNil(t, err)
		require.Equal(t, fmt.Sprintf("%s does not exist", sourcePath), err.Error())
	})
}

func TestListFilesInDirectory(t *testing.T) {
	tempDir := t.TempDir()
	files := make([]string, 5)
	for i := 0; i < 5; i++ {
		fileDir := path.Join(tempDir, fmt.Sprintf("%d", i))
		err := os.MkdirAll(fileDir, os.ModePerm)
		require.Nil(t, err)
		ext := ".txt"
		if i%2 == 0 {
			ext = ".csv"
		}
		filepath := path.Join(fileDir, fmt.Sprintf("random-%d%s", i, ext))
		files[i] = filepath
		_, err = os.Create(filepath)
		require.Nil(t, err)
	}

	t.Run("can list all files", func(t *testing.T) {
		mFiles, errs := utils.ListFilesInDirectory(tempDir, []string{"*"})
		require.Len(t, mFiles, 5)
		require.Empty(t, errs)
	})

	t.Run("can list by extension", func(t *testing.T) {
		mFiles, errs := utils.ListFilesInDirectory(tempDir, []string{"*.csv"})
		require.Len(t, mFiles, 3)
		require.Empty(t, errs)
		require.ElementsMatch(t, []string{files[0], files[2], files[4]}, mFiles)
	})

	t.Run("can list on multiple extensions files", func(t *testing.T) {
		mFiles, errs := utils.ListFilesInDirectory(tempDir, []string{"*.csv", "*.txt"})
		require.Len(t, mFiles, 5)
		require.Empty(t, errs)
	})

}
