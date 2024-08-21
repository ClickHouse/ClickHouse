package internal

import (
	c "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	o "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type runConfiguration struct {
	id               string
	host             string
	port             uint16
	username         string
	password         string
	output           string
	collectors       []string
	collectorConfigs map[string]config.Configuration
	outputConfig     config.Configuration
}

func NewRunConfiguration(id string, host string, port uint16, username string, password string, output string, outputConfig config.Configuration,
	collectors []string, collectorConfigs map[string]config.Configuration) *runConfiguration {
	config := runConfiguration{
		id:               id,
		host:             host,
		port:             port,
		username:         username,
		password:         password,
		collectors:       collectors,
		output:           output,
		collectorConfigs: collectorConfigs,
		outputConfig:     outputConfig,
	}
	return &config
}

func Capture(config *runConfiguration) {
	bundles, err := collect(config)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to perform collection")
	}
	log.Info().Msgf("collectors initialized")
	if err = output(config, bundles); err != nil {
		log.Fatal().Err(err).Msg("unable to create output")
	}
	log.Info().Msgf("bundle export complete")
}

func collect(config *runConfiguration) (map[string]*data.DiagnosticBundle, error) {
	resourceManager := platform.GetResourceManager()
	err := resourceManager.Connect(config.host, config.port, config.username, config.password)
	if err != nil {
		// if we can't connect this is fatal
		log.Fatal().Err(err).Msg("Unable to connect to database")
	}
	//grab the required connectors - we pass what we can
	bundles := make(map[string]*data.DiagnosticBundle)
	log.Info().Msgf("connection established")
	//these store our collection errors and will be output in the bundle
	var collectorErrors [][]interface{}
	for _, collectorName := range config.collectors {
		collectorConfig := config.collectorConfigs[collectorName]
		log.Info().Msgf("initializing %s collector", collectorName)
		collector, err := c.GetCollectorByName(collectorName)
		if err != nil {
			log.Error().Err(err).Msgf("Unable to fetch collector %s", collectorName)
			collectorErrors = append(collectorErrors, []interface{}{err.Error()})
			continue
		}
		bundle, err := collector.Collect(collectorConfig)
		if err != nil {
			log.Error().Err(err).Msgf("Error in collector %s", collectorName)
			collectorErrors = append(collectorErrors, []interface{}{err.Error()})
			// this indicates a fatal error in the collector
			continue
		}
		for _, fError := range bundle.Errors.Errors {
			err = errors.Wrapf(fError, "Failure to collect frame in collector %s", collectorName)
			collectorErrors = append(collectorErrors, []interface{}{err.Error()})
			log.Warn().Msg(err.Error())
		}
		bundles[collectorName] = bundle
	}
	bundles["diag_trace"] = buildTraceBundle(collectorErrors)
	return bundles, nil
}

func output(config *runConfiguration, bundles map[string]*data.DiagnosticBundle) error {
	log.Info().Msgf("attempting to export bundle using %s output...", config.output)
	output, err := o.GetOutputByName(config.output)
	if err != nil {
		return err
	}
	frameErrors, err := output.Write(config.id, bundles, config.outputConfig)
	// we report over failing hard on frame errors - up to the output to determine what is fatal via error
	for _, fError := range frameErrors.Errors {
		log.Warn().Msgf("failure to write frame in output %s - %s", config.output, fError)
	}
	return err
}

func buildTraceBundle(collectorErrors [][]interface{}) *data.DiagnosticBundle {
	errorBundle := data.DiagnosticBundle{
		Frames: map[string]data.Frame{
			"errors": data.NewMemoryFrame("errors", []string{"errors"}, collectorErrors),
		},
		Errors: data.FrameErrors{},
	}
	// add any other metrics from collection
	return &errorBundle
}
