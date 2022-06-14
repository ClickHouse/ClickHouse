package data_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
)

func TestNextFileDirectoryFrame(t *testing.T) {
	t.Run("can iterate file frame", func(t *testing.T) {
		tempDir := t.TempDir()
		files := make([]string, 5)
		for i := 0; i < 5; i++ {
			fileDir := path.Join(tempDir, fmt.Sprintf("%d", i))
			err := os.MkdirAll(fileDir, os.ModePerm)
			require.Nil(t, err)
			filepath := path.Join(fileDir, fmt.Sprintf("random-%d.txt", i))
			files[i] = filepath
			_, err = os.Create(filepath)
			require.Nil(t, err)
		}
		fileFrame, errs := data.NewFileDirectoryFrame(tempDir, []string{"*.txt"})
		require.Empty(t, errs)
		i := 0
		for {
			values, ok, err := fileFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Len(t, values, 1)
			require.Equal(t, files[i], values[0].(data.SimpleFile).Path)
			i += 1
		}
		require.Equal(t, 5, i)
	})

	t.Run("can iterate file frame when empty", func(t *testing.T) {
		// create 5 temporary files
		tempDir := t.TempDir()
		fileFrame, errs := data.NewFileDirectoryFrame(tempDir, []string{"*"})
		require.Empty(t, errs)
		i := 0
		for {
			_, ok, err := fileFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
		}
		require.Equal(t, 0, i)
	})
}

func TestNewConfigFileFrame(t *testing.T) {
	t.Run("can iterate config file frame", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)

		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "xml"))
		require.Empty(t, errs)
		i := 0
		for {
			values, ok, err := configFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Len(t, values, 1)
			filePath := values[0].(data.XmlConfigFile).FilePath()
			require.True(t, strings.Contains(filePath, ".xml"))
			i += 1
		}
		// 5 not 3 due to the includes
		require.Equal(t, 5, i)
	})

	t.Run("can iterate file frame when empty", func(t *testing.T) {
		// create 5 temporary files
		tempDir := t.TempDir()
		configFrame, errs := data.NewConfigFileFrame(tempDir)
		require.Empty(t, errs)
		i := 0
		for {
			_, ok, err := configFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
		}
		require.Equal(t, 0, i)
	})
}

func TestConfigFileFrameCopy(t *testing.T) {
	t.Run("can copy non-sensitive xml config files", func(t *testing.T) {
		tmrDir := t.TempDir()
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "xml"))
		require.Empty(t, errs)
		for {
			values, ok, err := configFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Nil(t, err)
			require.True(t, ok)
			configFile := values[0].(data.XmlConfigFile)
			newPath := path.Join(tmrDir, filepath.Base(configFile.FilePath()))
			err = configFile.Copy(newPath, false)
			require.FileExists(t, newPath)
			sourceInfo, _ := os.Stat(configFile.FilePath())
			destInfo, _ := os.Stat(newPath)
			require.Equal(t, sourceInfo.Size(), destInfo.Size())
			require.Nil(t, err)
		}
	})

	t.Run("can copy sensitive xml config files", func(t *testing.T) {
		tmrDir := t.TempDir()
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "xml"))
		require.Empty(t, errs)
		i := 0
		sizes := map[string]int64{
			"users.xml":            int64(2039),
			"default-password.xml": int64(188),
			"config.xml":           int64(61282),
			"server-include.xml":   int64(168),
			"user-include.xml":     int64(582),
		}
		var checkedFiles []string
		for {
			values, ok, err := configFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Nil(t, err)
			require.True(t, ok)
			configFile := values[0].(data.XmlConfigFile)
			fileName := filepath.Base(configFile.FilePath())
			newPath := path.Join(tmrDir, fileName)
			err = configFile.Copy(newPath, true)
			require.FileExists(t, newPath)
			destInfo, _ := os.Stat(newPath)
			require.Equal(t, sizes[fileName], destInfo.Size())
			require.Nil(t, err)
			bytes, err := ioutil.ReadFile(newPath)
			require.Nil(t, err)
			s := string(bytes)
			checkedFiles = append(checkedFiles, fileName)
			if fileName == "users.xml" || fileName == "default-password.xml" || fileName == "user-include.xml" {
				require.True(t, strings.Contains(s, "<password>Replaced</password>") ||
					strings.Contains(s, "<password_sha256_hex>Replaced</password_sha256_hex>"))
				require.NotContains(t, s, "<password>REPLACE_ME</password>")
				require.NotContains(t, s, "<password_sha256_hex>REPLACE_ME</password_sha256_hex>")
			} else if fileName == "config.xml" {
				require.True(t, strings.Contains(s, "<access_key_id>Replaced</access_key_id>"))
				require.True(t, strings.Contains(s, "<secret_access_key>Replaced</secret_access_key>"))
				require.True(t, strings.Contains(s, "<secret>Replaced</secret>"))
				require.NotContains(t, s, "<access_key_id>REPLACE_ME</access_key_id>")
				require.NotContains(t, s, "<secret_access_key>REPLACE_ME</secret_access_key>")
				require.NotContains(t, s, "<secret>REPLACE_ME</secret>")
			}
			i++
		}
		require.ElementsMatch(t, []string{"users.xml", "default-password.xml", "user-include.xml", "config.xml", "server-include.xml"}, checkedFiles)
		require.Equal(t, 5, i)
	})

	t.Run("can copy sensitive yaml config files", func(t *testing.T) {
		tmrDir := t.TempDir()
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "yaml"))
		require.Empty(t, errs)
		i := 0
		sizes := map[string]int64{
			"users.yaml":            int64(1023),
			"default-password.yaml": int64(132),
			"config.yaml":           int64(42512),
			"server-include.yaml":   int64(21),
			"user-include.yaml":     int64(120),
		}
		var checkedFiles []string
		for {
			values, ok, err := configFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Nil(t, err)
			require.True(t, ok)
			configFile := values[0].(data.YamlConfigFile)
			fileName := filepath.Base(configFile.FilePath())
			newPath := path.Join(tmrDir, fileName)
			err = configFile.Copy(newPath, true)
			require.FileExists(t, newPath)
			destInfo, _ := os.Stat(newPath)
			require.Equal(t, sizes[fileName], destInfo.Size())
			require.Nil(t, err)
			bytes, err := ioutil.ReadFile(newPath)
			require.Nil(t, err)
			s := string(bytes)
			checkedFiles = append(checkedFiles, fileName)
			if fileName == "users.yaml" || fileName == "default-password.yaml" || fileName == "user-include.yaml" {
				require.True(t, strings.Contains(s, "password: 'Replaced'") ||
					strings.Contains(s, "password_sha256_hex: 'Replaced'"))
				require.NotContains(t, s, "password: 'REPLACE_ME'")
				require.NotContains(t, s, "password_sha256_hex: \"REPLACE_ME\"")
			} else if fileName == "config.yaml" {
				require.True(t, strings.Contains(s, "access_key_id: 'Replaced'"))
				require.True(t, strings.Contains(s, "secret_access_key: 'Replaced'"))
				require.True(t, strings.Contains(s, "secret: 'Replaced'"))
				require.NotContains(t, s, "access_key_id: 'REPLACE_ME'")
				require.NotContains(t, s, "secret_access_key: REPLACE_ME")
				require.NotContains(t, s, "secret: REPLACE_ME")
			}
			i++
		}
		require.ElementsMatch(t, []string{"users.yaml", "default-password.yaml", "user-include.yaml", "config.yaml", "server-include.yaml"}, checkedFiles)
		require.Equal(t, 5, i)
	})
}

func TestConfigFileFrameFindLogPaths(t *testing.T) {
	t.Run("can find xml log paths", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "xml"))
		require.Empty(t, errs)
		paths, errs := configFrame.FindLogPaths()
		require.Empty(t, errs)
		require.ElementsMatch(t, []string{"/var/log/clickhouse-server/clickhouse-server.log",
			"/var/log/clickhouse-server/clickhouse-server.err.log"}, paths)
	})

	t.Run("can handle empty log paths", func(t *testing.T) {
		configFrame, errs := data.NewConfigFileFrame(t.TempDir())
		require.Empty(t, errs)
		paths, errs := configFrame.FindLogPaths()
		require.Empty(t, errs)
		require.Empty(t, paths)
	})

	t.Run("can find yaml log paths", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "yaml"))
		require.Empty(t, errs)
		paths, errs := configFrame.FindLogPaths()
		require.Empty(t, errs)
		require.ElementsMatch(t, []string{"/var/log/clickhouse-server/clickhouse-server.log",
			"/var/log/clickhouse-server/clickhouse-server.err.log"}, paths)
	})
}

// test the legacy format for ClickHouse xml config files with a yandex root tag
func TestYandexConfigFile(t *testing.T) {
	t.Run("can find xml log paths with yandex root", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)
		configFrame, errs := data.NewConfigFileFrame(path.Join(cwd, "../../../testdata", "configs", "yandex_xml"))
		require.Empty(t, errs)
		paths, errs := configFrame.FindLogPaths()
		require.Empty(t, errs)
		require.ElementsMatch(t, []string{"/var/log/clickhouse-server/clickhouse-server.log",
			"/var/log/clickhouse-server/clickhouse-server.err.log"}, paths)
	})
}
