package collectors

import (
	"fmt"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Collector interface {
	Collect(config config.Configuration) (*data.DiagnosticBundle, error)
	Configuration() config.Configuration
	IsDefault() bool
	Description() string
}

// Register can be called from init() on a collector in this package
// It will automatically be added to the Collectors map to be called externally
func Register(name string, collector CollectorFactory) {
	if name == "diag_trace" {
		// we use this to record errors and warnings
		log.Fatal().Msgf("diag_trace is a reserved collector name")
	}
	// names must be unique
	if _, ok := Collectors[name]; ok {
		log.Fatal().Msgf("More than 1 collector is trying to register under the name %s. Names must be unique.", name)
	}
	Collectors[name] = collector
}

// CollectorFactory lets us use a closure to get instances of the collector struct
type CollectorFactory func() (Collector, error)

var Collectors = map[string]CollectorFactory{}

func GetCollectorNames(defaultOnly bool) []string {
	// can't pre-allocate as not all maybe default
	var collectors []string
	for collectorName := range Collectors {
		collector, err := GetCollectorByName(collectorName)
		if err != nil {
			log.Fatal().Err(err)
		}
		if !defaultOnly || (defaultOnly && collector.IsDefault()) {
			collectors = append(collectors, collectorName)
		}
	}
	return collectors
}

func GetCollectorByName(name string) (Collector, error) {
	if collectorFactory, ok := Collectors[name]; ok {
		//do something here
		collector, err := collectorFactory()
		if err != nil {
			return nil, errors.Wrapf(err, "collector %s could not be initialized", name)
		}
		return collector, nil
	}
	return nil, fmt.Errorf("%s is not a valid collector name", name)
}

func BuildConfigurationOptions() (map[string]config.Configuration, error) {
	configurations := make(map[string]config.Configuration)
	for name, collectorFactory := range Collectors {
		collector, err := collectorFactory()
		if err != nil {
			return nil, errors.Wrapf(err, "collector %s could not be initialized", name)
		}
		configurations[name] = collector.Configuration()
	}
	return configurations, nil
}
