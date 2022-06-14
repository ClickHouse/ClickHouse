package file_test

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs/file"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
)

var clusterFrame = test.NewFakeDataFrame("clusters", []string{"cluster", "shard_num", "shard_weight", "replica_num", "host_name", "host_address", "port", "is_local", "user", "default_database", "errors_count", "slowdowns_count", "estimated_recovery_time"},
	[][]interface{}{
		{"events", 1, 1, 1, "dalem-local-clickhouse-blue-1", "192.168.144.2", 9000, 1, "default", "", 0, 0, 0},
		{"events", 2, 1, 1, "dalem-local-clickhouse-blue-2", "192.168.144.4", 9001, 1, "default", "", 0, 0, 0},
		{"events", 3, 1, 1, "dalem-local-clickhouse-blue-3", "192.168.144.3", 9002, 1, "default", "", 0, 0, 0},
	},
)

var diskFrame = test.NewFakeDataFrame("disks", []string{"name", "path", "free_space", "total_space", "keep_free_space", "type"},
	[][]interface{}{
		{"default", "/var/lib/clickhouse", 1729659346944, 1938213220352, "", "local"},
	},
)

var userFrame = test.NewFakeDataFrame("users", []string{"name", "id", "storage", "auth_type", "auth_params", "host_ip", "host_names", "host_names_regexp", "host_names_like"},
	[][]interface{}{
		{"default", "94309d50-4f52-5250-31bd-74fecac179db,users.xml,plaintext_password", "sha256_password", []string{"::0"}, []string{}, []string{}, []string{}},
	},
)

func TestConfiguration(t *testing.T) {
	t.Run("correct configuration is returned", func(t *testing.T) {
		output := file.SimpleOutput{}
		conf := output.Configuration()
		require.Len(t, conf.Params, 3)
		// check first directory param
		require.IsType(t, config.StringParam{}, conf.Params[0])
		directory, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.False(t, directory.Required())
		require.Equal(t, "directory", directory.Name())
		require.Equal(t, "./", directory.Value)
		// check second format param
		require.IsType(t, config.StringOptions{}, conf.Params[1])
		format, ok := conf.Params[1].(config.StringOptions)
		require.True(t, ok)
		require.False(t, format.Required())
		require.Equal(t, "format", format.Name())
		require.Equal(t, "csv", format.Value)
		require.Equal(t, []string{"csv"}, format.Options)
		// check third format compress
		require.IsType(t, config.BoolParam{}, conf.Params[2])
		skipArchive, ok := conf.Params[2].(config.BoolParam)
		require.True(t, ok)
		require.False(t, format.Required())
		require.False(t, skipArchive.Value)
	})
}

func TestWrite(t *testing.T) {
	bundles := map[string]*data.DiagnosticBundle{
		"systemA": {
			Frames: map[string]data.Frame{
				"disk":    diskFrame,
				"cluster": clusterFrame,
			},
		},
		"systemB": {
			Frames: map[string]data.Frame{
				"user": userFrame,
			},
		},
	}
	t.Run("test we can write simple diagnostic sets", func(t *testing.T) {
		tempDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: tempDir,
				},
				// turn compression off as the folder will be deleted by default
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", bundles, configuration)
		require.Nil(t, err)
		require.Equal(t, data.FrameErrors{}, frameErrors)
		clusterFile := path.Join(tempDir, "test", "test", "systemA", "cluster.csv")
		diskFile := path.Join(tempDir, "test", "test", "systemA", "disk.csv")
		userFile := path.Join(tempDir, "test", "test", "systemB", "user.csv")
		require.FileExists(t, clusterFile)
		require.FileExists(t, diskFile)
		require.FileExists(t, userFile)
		diskLines, err := readFileLines(diskFile)
		require.Nil(t, err)
		require.Len(t, diskLines, 2)
		usersLines, err := readFileLines(userFile)
		require.Nil(t, err)
		require.Len(t, usersLines, 2)
		clusterLines, err := readFileLines(clusterFile)
		require.Nil(t, err)
		require.Len(t, clusterLines, 4)
		require.Equal(t, strings.Join(clusterFrame.ColumnNames, ","), clusterLines[0])
		require.Equal(t, "events,1,1,1,dalem-local-clickhouse-blue-1,192.168.144.2,9000,1,default,,0,0,0", clusterLines[1])
		require.Equal(t, "events,2,1,1,dalem-local-clickhouse-blue-2,192.168.144.4,9001,1,default,,0,0,0", clusterLines[2])
		require.Equal(t, "events,3,1,1,dalem-local-clickhouse-blue-3,192.168.144.3,9002,1,default,,0,0,0", clusterLines[3])
		resetFrames()
	})

	t.Run("test invalid parameter", func(t *testing.T) {
		tempDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: tempDir,
				},
				config.StringOptions{
					Value:   "random",
					Options: []string{"csv"},
					// TODO: add tsv and others here later
					Param: config.NewParam("format", "Format of exported files", false),
				},
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip compressed archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", bundles, configuration)
		require.Equal(t, data.FrameErrors{}, frameErrors)
		require.NotNil(t, err)
		require.Equal(t, "parameter format is invalid - random is not a valid value for format - [csv]", err.Error())
		resetFrames()
	})

	t.Run("test compression", func(t *testing.T) {
		tempDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: tempDir,
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", bundles, configuration)
		require.Nil(t, err)
		require.Equal(t, data.FrameErrors{}, frameErrors)
		archiveFileName := path.Join(tempDir, "test", "test.tar.gz")
		fi, err := os.Stat(archiveFileName)
		require.Nil(t, err)
		require.FileExists(t, archiveFileName)
		// compression will vary so lets test range
		require.Greater(t, int64(600), fi.Size())
		require.Less(t, int64(200), fi.Size())
		outputFolder := path.Join(tempDir, "test", "test")
		// check the folder doesn't exist and is cleaned up
		require.NoFileExists(t, outputFolder)
		resetFrames()
	})

	t.Run("test support for directory frames", func(t *testing.T) {
		// create 5 temporary files
		tempDir := t.TempDir()
		files := createRandomFiles(tempDir, 5)
		dirFrame, errs := data.NewFileDirectoryFrame(tempDir, []string{"*.log"})
		require.Empty(t, errs)
		fileBundles := map[string]*data.DiagnosticBundle{
			"systemA": {
				Frames: map[string]data.Frame{
					"disk":    diskFrame,
					"cluster": clusterFrame,
				},
			},
			"config": {
				Frames: map[string]data.Frame{
					"logs": dirFrame,
				},
			},
		}
		destDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: destDir,
				},
				// turn compression off as the folder will be deleted by default
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", fileBundles, configuration)
		require.Nil(t, err)
		require.NotNil(t, frameErrors)

		// test the usual frames still work
		clusterFile := path.Join(destDir, "test", "test", "systemA", "cluster.csv")
		diskFile := path.Join(destDir, "test", "test", "systemA", "disk.csv")
		require.FileExists(t, clusterFile)
		require.FileExists(t, diskFile)
		diskLines, err := readFileLines(diskFile)
		require.Nil(t, err)
		require.Len(t, diskLines, 2)
		clusterLines, err := readFileLines(clusterFile)
		require.Nil(t, err)
		require.Len(t, clusterLines, 4)
		require.Equal(t, strings.Join(clusterFrame.ColumnNames, ","), clusterLines[0])
		require.Equal(t, "events,1,1,1,dalem-local-clickhouse-blue-1,192.168.144.2,9000,1,default,,0,0,0", clusterLines[1])
		require.Equal(t, "events,2,1,1,dalem-local-clickhouse-blue-2,192.168.144.4,9001,1,default,,0,0,0", clusterLines[2])
		require.Equal(t, "events,3,1,1,dalem-local-clickhouse-blue-3,192.168.144.3,9002,1,default,,0,0,0", clusterLines[3])
		//test our directory frame
		for _, filepath := range files {
			// check they were copied
			subPath := strings.TrimPrefix(filepath, tempDir)
			// path here will be <destDir>/<id>/test>/config/logs/<sub path>
			newPath := path.Join(destDir, "test", "test", "config", "logs", subPath)
			require.FileExists(t, newPath)
		}
		resetFrames()
	})

	t.Run("test support for config frames", func(t *testing.T) {
		xmlConfig := data.XmlConfig{
			XMLName: xml.Name{},
			Clickhouse: data.XmlLoggerConfig{
				XMLName:  xml.Name{},
				ErrorLog: "/var/log/clickhouse-server/clickhouse-server.err.log",
				Log:      "/var/log/clickhouse-server/clickhouse-server.log",
			},
			IncludeFrom: "",
		}
		tempDir := t.TempDir()
		confDir := path.Join(tempDir, "conf")
		// create an includes file
		includesDir := path.Join(tempDir, "includes")
		err := os.MkdirAll(includesDir, os.ModePerm)
		require.Nil(t, err)
		includesPath := path.Join(includesDir, "random.xml")
		includesFile, err := os.Create(includesPath)
		require.Nil(t, err)
		xmlWriter := io.Writer(includesFile)
		enc := xml.NewEncoder(xmlWriter)
		enc.Indent("  ", "    ")
		err = enc.Encode(xmlConfig)
		require.Nil(t, err)
		// create 5 temporary config files
		files := make([]string, 5)
		// set the includes
		xmlConfig.IncludeFrom = includesPath
		for i := 0; i < 5; i++ {
			// we want to check hierarchies are preserved so create a simple folder for each file
			fileDir := path.Join(confDir, fmt.Sprintf("%d", i))
			err := os.MkdirAll(fileDir, os.ModePerm)
			require.Nil(t, err)
			filepath := path.Join(fileDir, fmt.Sprintf("random-%d.xml", i))
			files[i] = filepath
			xmlFile, err := os.Create(filepath)
			require.Nil(t, err)
			// write a little xml so its valid
			xmlWriter := io.Writer(xmlFile)
			enc := xml.NewEncoder(xmlWriter)
			enc.Indent("  ", "    ")
			err = enc.Encode(xmlConfig)
			require.Nil(t, err)
		}
		configFrame, errs := data.NewConfigFileFrame(confDir)
		require.Empty(t, errs)
		fileBundles := map[string]*data.DiagnosticBundle{
			"systemA": {
				Frames: map[string]data.Frame{
					"disk":    diskFrame,
					"cluster": clusterFrame,
				},
			},
			"config": {
				Frames: map[string]data.Frame{
					"user_specified": configFrame,
				},
			},
		}
		destDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: destDir,
				},
				// turn compression off as the folder will be deleted by default
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", fileBundles, configuration)
		require.Nil(t, err)
		require.NotNil(t, frameErrors)
		require.Empty(t, frameErrors.Errors)
		//test our config frame
		for _, filepath := range files {
			// check they were copied
			subPath := strings.TrimPrefix(filepath, confDir)
			// path here will be <destDir>/<id>/test>/config/user_specified/file
			newPath := path.Join(destDir, "test", "test", "config", "user_specified", subPath)
			require.FileExists(t, newPath)
		}
		// check our includes file exits
		// path here will be <destDir>/<id>/test>/config/user_specified/file/includes
		require.FileExists(t, path.Join(destDir, "test", "test", "config", "user_specified", "includes", includesPath))
		resetFrames()
	})

	t.Run("test support for file frames", func(t *testing.T) {
		// create 5 temporary files
		tempDir := t.TempDir()
		files := createRandomFiles(tempDir, 5)
		fileFrame := data.NewFileFrame("collection", files)
		fileBundles := map[string]*data.DiagnosticBundle{
			"systemA": {
				Frames: map[string]data.Frame{
					"disk":    diskFrame,
					"cluster": clusterFrame,
				},
			},
			"file": {
				Frames: map[string]data.Frame{
					"collection": fileFrame,
				},
			},
		}
		destDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: destDir,
				},
				// turn compression off as the folder will be deleted by default
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		frameErrors, err := output.Write("test", fileBundles, configuration)
		require.Nil(t, err)
		require.NotNil(t, frameErrors)
		//test our directory frame
		for _, filepath := range files {
			// path here will be <destDir>/<id>/test>/file/collection/<sub path>
			newPath := path.Join(destDir, "test", "test", "file", "collection", filepath)
			require.FileExists(t, newPath)
		}
		resetFrames()
	})

	t.Run("test support for hierarchical frames", func(t *testing.T) {
		bottomFrame := data.NewHierarchicalFrame("bottomLevel", userFrame, []data.HierarchicalFrame{})
		middleFrame := data.NewHierarchicalFrame("middleLevel", diskFrame, []data.HierarchicalFrame{bottomFrame})
		topFrame := data.NewHierarchicalFrame("topLevel", clusterFrame, []data.HierarchicalFrame{middleFrame})
		tempDir := t.TempDir()
		configuration := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Param: config.NewParam("directory", "A directory", true),
					Value: tempDir,
				},
				// turn compression off as the folder will be deleted by default
				config.BoolParam{
					Value: true,
					Param: config.NewParam("skip_archive", "Skip archive", false),
				},
			},
		}
		output := file.SimpleOutput{FolderGenerator: staticFolderName}
		hierarchicalBundle := map[string]*data.DiagnosticBundle{
			"systemA": {
				Frames: map[string]data.Frame{
					"topLevel": topFrame,
				},
			},
		}
		frameErrors, err := output.Write("test", hierarchicalBundle, configuration)
		require.Nil(t, err)
		require.Equal(t, data.FrameErrors{}, frameErrors)
		topFile := path.Join(tempDir, "test", "test", "systemA", "topLevel.csv")
		middleFile := path.Join(tempDir, "test", "test", "systemA", "middleLevel", "middleLevel.csv")
		bottomFile := path.Join(tempDir, "test", "test", "systemA", "middleLevel", "bottomLevel", "bottomLevel.csv")
		require.FileExists(t, topFile)
		require.FileExists(t, middleFile)
		require.FileExists(t, bottomFile)
		topLines, err := readFileLines(topFile)
		require.Nil(t, err)
		require.Len(t, topLines, 4)
		middleLines, err := readFileLines(middleFile)
		require.Nil(t, err)
		require.Len(t, middleLines, 2)
		bottomLines, err := readFileLines(bottomFile)
		require.Nil(t, err)
		require.Len(t, bottomLines, 2)
		require.Equal(t, strings.Join(clusterFrame.ColumnNames, ","), topLines[0])
		require.Equal(t, "events,1,1,1,dalem-local-clickhouse-blue-1,192.168.144.2,9000,1,default,,0,0,0", topLines[1])
		require.Equal(t, "events,2,1,1,dalem-local-clickhouse-blue-2,192.168.144.4,9001,1,default,,0,0,0", topLines[2])
		require.Equal(t, "events,3,1,1,dalem-local-clickhouse-blue-3,192.168.144.3,9002,1,default,,0,0,0", topLines[3])
		resetFrames()
	})
}

func createRandomFiles(tempDir string, num int) []string {
	files := make([]string, num)
	for i := 0; i < 5; i++ {
		// we want to check hierarchies are preserved so create a simple folder for each file
		fileDir := path.Join(tempDir, fmt.Sprintf("%d", i))
		os.MkdirAll(fileDir, os.ModePerm) //nolint:errcheck
		filepath := path.Join(fileDir, fmt.Sprintf("random-%d.log", i))
		files[i] = filepath
		os.Create(filepath) //nolint:errcheck
	}
	return files
}

func resetFrames() {
	clusterFrame.Reset()
	userFrame.Reset()
	diskFrame.Reset()
}

func readFileLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func staticFolderName() string {
	return "test"
}
