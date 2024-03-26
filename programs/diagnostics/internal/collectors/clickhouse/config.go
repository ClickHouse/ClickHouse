package clickhouse

import (
	"fmt"
	"path/filepath"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/pkg/errors"
)

type ConfigCollector struct {
	resourceManager *platform.ResourceManager
}

func NewConfigCollector(m *platform.ResourceManager) *ConfigCollector {
	return &ConfigCollector{
		resourceManager: m,
	}
}

const DefaultConfigLocation = "/etc/clickhouse-server/"
const ProcessedConfigurationLocation = "/var/lib/clickhouse/preprocessed_configs"

func (c ConfigCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(c.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	directory, err := config.ReadStringValue(conf, "directory")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}

	if directory != "" {
		// user has specified a directory - we therefore skip all other efforts to locate the config
		frame, errs := data.NewConfigFileFrame(directory)
		return &data.DiagnosticBundle{
			Frames: map[string]data.Frame{
				"user_specified": frame,
			},
			Errors: data.FrameErrors{Errors: errs},
		}, nil
	}
	configCandidates, err := FindConfigurationFiles()
	if err != nil {
		return &data.DiagnosticBundle{}, errors.Wrapf(err, "Unable to find configuration files")
	}
	frames := make(map[string]data.Frame)
	var frameErrors []error
	for frameName, confDir := range configCandidates {
		frame, errs := data.NewConfigFileFrame(confDir)
		frameErrors = append(frameErrors, errs...)
		frames[frameName] = frame
	}
	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: data.FrameErrors{Errors: frameErrors},
	}, err
}

func FindConfigurationFiles() (map[string]string, error) {
	configCandidates := map[string]string{
		"default":      DefaultConfigLocation,
		"preprocessed": ProcessedConfigurationLocation,
	}
	// we don't know specifically where the config is but try to find via processes
	processConfigs, err := utils.FindConfigsFromClickHouseProcesses()
	if err != nil {
		return nil, err
	}
	for i, path := range processConfigs {
		confDir := filepath.Dir(path)
		if len(processConfigs) == 1 {
			configCandidates["process"] = confDir
			break
		}
		configCandidates[fmt.Sprintf("process_%d", i)] = confDir
	}
	return configCandidates, nil
}

func (c ConfigCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value:      "",
				Param:      config.NewParam("directory", "Specify the location of the configuration files for ClickHouse Server e.g. /etc/clickhouse-server/", false),
				AllowEmpty: true,
			},
		},
	}
}

func (c ConfigCollector) Description() string {
	return "Collects the ClickHouse configuration from the local filesystem."
}

func (c ConfigCollector) IsDefault() bool {
	return true
}

// here we register the collector for use
func init() {
	collectors.Register("config", func() (collectors.Collector, error) {
		return &ConfigCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
