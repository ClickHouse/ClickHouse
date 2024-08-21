package system

import (
	"os"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/yargevad/filepathx"
)

// This collector collects arbitrary user files

type FileCollector struct {
	resourceManager *platform.ResourceManager
}

func NewFileCollector(m *platform.ResourceManager) *FileCollector {
	return &FileCollector{
		resourceManager: m,
	}
}

func (f *FileCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(f.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	filePattern, err := config.ReadStringValue(conf, "file_pattern")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}

	var frameErrors []error
	// this util package supports recursive file matching e.g. /**/*
	matches, err := filepathx.Glob(filePattern)
	if err != nil {
		return &data.DiagnosticBundle{}, errors.Wrapf(err, "Invalid file_pattern \"%s\"", filePattern)
	}

	if len(matches) == 0 {
		frameErrors = append(frameErrors, errors.New("0 files match glob pattern"))
		return &data.DiagnosticBundle{
			Errors: data.FrameErrors{Errors: frameErrors},
		}, nil
	}

	var filePaths []string
	for _, match := range matches {
		fi, err := os.Stat(match)
		if err != nil {
			frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to read file %s", match))
		}
		if !fi.IsDir() {
			log.Debug().Msgf("Collecting file %s", match)
			filePaths = append(filePaths, match)
		}
	}

	frame := data.NewFileFrame("collection", filePaths)

	return &data.DiagnosticBundle{
		Errors: data.FrameErrors{Errors: frameErrors},
		Frames: map[string]data.Frame{
			"collection": frame,
		},
	}, nil
}

func (f *FileCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value:      "",
				Param:      config.NewParam("file_pattern", "Glob based pattern to specify files for collection", true),
				AllowEmpty: false,
			},
		},
	}
}

func (f *FileCollector) IsDefault() bool {
	return false
}

func (f *FileCollector) Description() string {
	return "Allows collection of user specified files"
}

// here we register the collector for use
func init() {
	collectors.Register("file", func() (collectors.Collector, error) {
		return &FileCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
