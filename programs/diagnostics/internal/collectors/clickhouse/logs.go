package clickhouse

import (
	"fmt"
	"path/filepath"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
)

// This collector collects logs

type LogsCollector struct {
	resourceManager *platform.ResourceManager
}

func NewLogsCollector(m *platform.ResourceManager) *LogsCollector {
	return &LogsCollector{
		resourceManager: m,
	}
}

var DefaultLogsLocation = filepath.Clean("/var/log/clickhouse-server/")

func (lc *LogsCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(lc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	directory, err := config.ReadStringValue(conf, "directory")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	collectArchives, err := config.ReadBoolValue(conf, "collect_archives")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	logPatterns := []string{"*.log"}
	if collectArchives {
		logPatterns = append(logPatterns, "*.gz")
	}

	if directory != "" {
		// user has specified a directory - we therefore skip all other efforts to locate the logs
		frame, errs := data.NewFileDirectoryFrame(directory, logPatterns)
		return &data.DiagnosticBundle{
			Frames: map[string]data.Frame{
				"user_specified": frame,
			},
			Errors: data.FrameErrors{Errors: errs},
		}, nil
	}
	// add the default
	frames := make(map[string]data.Frame)
	dirFrame, frameErrors := data.NewFileDirectoryFrame(DefaultLogsLocation, logPatterns)
	frames["default"] = dirFrame
	logFolders, errs := FindLogFileCandidates()
	frameErrors = append(frameErrors, errs...)
	i := 0
	for folder, paths := range logFolders {
		// we will collect the default location anyway above so skip these
		if folder != DefaultLogsLocation {
			if collectArchives {
				paths = append(paths, "*.gz")
			}
			dirFrame, errs := data.NewFileDirectoryFrame(folder, paths)
			frames[fmt.Sprintf("logs-%d", i)] = dirFrame
			frameErrors = append(frameErrors, errs...)
		}
	}
	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: data.FrameErrors{Errors: frameErrors},
	}, err
}

func (lc *LogsCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value:      "",
				Param:      config.NewParam("directory", "Specify the location of the log files for ClickHouse Server e.g. /var/log/clickhouse-server/", false),
				AllowEmpty: true,
			},
			config.BoolParam{
				Param: config.NewParam("collect_archives", "Collect compressed log archive files", false),
			},
		},
	}
}

func FindLogFileCandidates() (logFolders map[string][]string, configErrors []error) {
	// we need the config to determine the location of the logs
	configCandidates := make(map[string]data.ConfigFileFrame)
	configFiles, err := FindConfigurationFiles()
	logFolders = make(map[string][]string)
	if err != nil {
		configErrors = append(configErrors, err)
		return logFolders, configErrors
	}
	for _, folder := range configFiles {
		configFrame, errs := data.NewConfigFileFrame(folder)
		configErrors = append(configErrors, errs...)
		configCandidates[filepath.Clean(folder)] = configFrame
	}

	for _, config := range configCandidates {
		paths, errs := config.FindLogPaths()
		for _, path := range paths {
			folder := filepath.Dir(path)
			filename := filepath.Base(path)
			if _, ok := logFolders[folder]; !ok {
				logFolders[folder] = []string{}
			}
			logFolders[folder] = utils.Unique(append(logFolders[folder], filename))
		}
		configErrors = append(configErrors, errs...)
	}
	return logFolders, configErrors
}

func (lc *LogsCollector) IsDefault() bool {
	return true
}

func (lc LogsCollector) Description() string {
	return "Collects the ClickHouse logs directly from the database."
}

// here we register the collector for use
func init() {
	collectors.Register("logs", func() (collectors.Collector, error) {
		return &LogsCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
