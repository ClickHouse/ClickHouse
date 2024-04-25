package clickhouse_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
)

func TestLogsConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for logs collector", func(t *testing.T) {
		client := test.NewFakeClickhouseClient(make(map[string][]string))
		logsCollector := clickhouse.NewLogsCollector(&platform.ResourceManager{
			DbClient: client,
		})
		conf := logsCollector.Configuration()
		require.Len(t, conf.Params, 2)
		// check directory
		require.IsType(t, config.StringParam{}, conf.Params[0])
		directory, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.False(t, directory.Required())
		require.Equal(t, directory.Name(), "directory")
		require.Empty(t, directory.Value)
		// check collect_archives
		require.IsType(t, config.BoolParam{}, conf.Params[1])
		collectArchives, ok := conf.Params[1].(config.BoolParam)
		require.True(t, ok)
		require.False(t, collectArchives.Required())
		require.Equal(t, collectArchives.Name(), "collect_archives")
		require.False(t, collectArchives.Value)
	})
}

func TestLogsCollect(t *testing.T) {

	logsCollector := clickhouse.NewLogsCollector(&platform.ResourceManager{})

	t.Run("test default logs collection", func(t *testing.T) {
		// we can't rely on a local installation of clickhouse being present for tests - if it is present (and running)
		// results maybe variable e.g. we may find a config. For now, we allow flexibility and test only default.
		// TODO: we may want to test this within a container
		bundle, err := logsCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, bundle)
		// we will have some errors if clickhouse is installed or not. If former, permission issues - if latter missing folders.
		require.Greater(t, len(bundle.Errors.Errors), 0)
		require.Len(t, bundle.Frames, 1)
		require.Contains(t, bundle.Frames, "default")
		_, ok := bundle.Frames["default"].(data.DirectoryFileFrame)
		require.True(t, ok)
		// no guarantees clickhouse is installed so this bundle could have no frames
	})

	t.Run("test logs collection when directory is specified", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)
		logsPath := path.Join(cwd, "../../../testdata", "logs", "var", "logs")
		bundle, err := logsCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      logsPath,
					Param:      config.NewParam("directory", "Specify the location of the log files for ClickHouse Server e.g. /var/log/clickhouse-server/", false),
					AllowEmpty: true,
				},
			},
		})
		require.Nil(t, err)
		checkDirectoryBundle(t, bundle, logsPath, []string{"clickhouse-server.log", "clickhouse-server.err.log"})

	})

	t.Run("test logs collection of archives", func(t *testing.T) {
		cwd, err := os.Getwd()
		require.Nil(t, err)
		logsPath := path.Join(cwd, "../../../testdata", "logs", "var", "logs")
		bundle, err := logsCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      logsPath,
					Param:      config.NewParam("directory", "Specify the location of the log files for ClickHouse Server e.g. /var/log/clickhouse-server/", false),
					AllowEmpty: true,
				},
				config.BoolParam{
					Value: true,
					Param: config.NewParam("collect_archives", "Collect compressed log archive files", false),
				},
			},
		})
		require.Nil(t, err)
		checkDirectoryBundle(t, bundle, logsPath, []string{"clickhouse-server.log", "clickhouse-server.err.log", "clickhouse-server.log.gz"})
	})

	t.Run("test when directory does not exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		logsPath := path.Join(tmpDir, "random")
		bundle, err := logsCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      logsPath,
					Param:      config.NewParam("directory", "Specify the location of the log files for ClickHouse Server e.g. /var/log/clickhouse-server/", false),
					AllowEmpty: true,
				},
			},
		})
		// not a fatal error currently
		require.Nil(t, err)
		require.Len(t, bundle.Errors.Errors, 1)
		require.Equal(t, fmt.Sprintf("directory %s does not exist", logsPath), bundle.Errors.Errors[0].Error())
	})
}

func checkDirectoryBundle(t *testing.T, bundle *data.DiagnosticBundle, logsPath string, expectedFiles []string) {
	require.NotNil(t, bundle)
	require.Nil(t, bundle.Errors.Errors)
	require.Len(t, bundle.Frames, 1)
	require.Contains(t, bundle.Frames, "user_specified")
	dirFrame, ok := bundle.Frames["user_specified"].(data.DirectoryFileFrame)
	require.True(t, ok)
	require.Equal(t, logsPath, dirFrame.Directory)
	require.Equal(t, []string{"files"}, dirFrame.Columns())
	i := 0
	fullPaths := make([]string, len(expectedFiles))
	for i, filePath := range expectedFiles {
		fullPaths[i] = path.Join(logsPath, filePath)
	}
	for {
		values, ok, err := dirFrame.Next()
		require.Nil(t, err)
		if !ok {
			break
		}
		require.Len(t, values, 1)
		file, ok := values[0].(data.SimpleFile)
		require.True(t, ok)
		require.Contains(t, fullPaths, file.FilePath())
		i += 1
	}
	require.Equal(t, len(fullPaths), i)
}
