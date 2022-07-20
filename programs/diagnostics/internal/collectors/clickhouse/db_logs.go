package clickhouse

import (
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/pkg/errors"
)

type DBLogTable struct {
	orderBy        data.OrderBy
	excludeColumns []string
}

var DbLogTables = map[string]DBLogTable{
	"query_log": {
		orderBy: data.OrderBy{
			Column: "event_time_microseconds",
			Order:  data.Asc,
		},
		excludeColumns: []string{},
	},
	"query_thread_log": {
		orderBy: data.OrderBy{
			Column: "event_time_microseconds",
			Order:  data.Asc,
		},
		excludeColumns: []string{},
	},
	"text_log": {
		orderBy: data.OrderBy{
			Column: "event_time_microseconds",
			Order:  data.Asc,
		},
		excludeColumns: []string{},
	},
}

// This collector collects db logs

type DBLogsCollector struct {
	resourceManager *platform.ResourceManager
}

func NewDBLogsCollector(m *platform.ResourceManager) *DBLogsCollector {
	return &DBLogsCollector{
		resourceManager: m,
	}
}

func (dc *DBLogsCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(dc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	rowLimit, err := config.ReadIntValue(conf, "row_limit")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}

	frames := make(map[string]data.Frame)
	var frameErrors []error
	for logTable, tableConfig := range DbLogTables {
		frame, err := dc.resourceManager.DbClient.ReadTable("system", logTable, tableConfig.excludeColumns, tableConfig.orderBy, rowLimit)
		if err != nil {
			frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to collect %s", logTable))
		} else {
			frames[logTable] = frame
		}
	}

	fErrors := data.FrameErrors{
		Errors: frameErrors,
	}
	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: fErrors,
	}, nil
}

func (dc *DBLogsCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.IntParam{
				Value: 100000,
				Param: config.NewParam("row_limit", "Maximum number of log rows to collect. Negative values mean unlimited", false),
			},
		},
	}
}

func (dc *DBLogsCollector) IsDefault() bool {
	return true
}

func (dc DBLogsCollector) Description() string {
	return "Collects the ClickHouse logs directly from the database."
}

// here we register the collector for use
func init() {
	collectors.Register("db_logs", func() (collectors.Collector, error) {
		return &DBLogsCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
