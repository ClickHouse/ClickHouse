package collectors_test

import (
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/clickhouse"
	_ "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/system"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/stretchr/testify/require"
)

func TestGetCollectorNames(t *testing.T) {
	t.Run("can get all collector names", func(t *testing.T) {
		collectorNames := collectors.GetCollectorNames(false)
		require.ElementsMatch(t, []string{"system_db", "config", "summary", "system", "logs", "db_logs", "file", "command", "zookeeper_db"}, collectorNames)
	})

	t.Run("can get default collector names", func(t *testing.T) {
		collectorNames := collectors.GetCollectorNames(true)
		require.ElementsMatch(t, []string{"system_db", "config", "summary", "system", "logs", "db_logs"}, collectorNames)
	})
}

func TestGetCollectorByName(t *testing.T) {

	t.Run("can get collector by name", func(t *testing.T) {
		collector, err := collectors.GetCollectorByName("system_db")
		require.Nil(t, err)
		require.Equal(t, clickhouse.NewSystemDatabaseCollector(platform.GetResourceManager()), collector)
	})

	t.Run("fails on non existing collector", func(t *testing.T) {
		collector, err := collectors.GetCollectorByName("random")
		require.NotNil(t, err)
		require.Equal(t, "random is not a valid collector name", err.Error())
		require.Nil(t, collector)
	})
}

func TestBuildConfigurationOptions(t *testing.T) {

	t.Run("can get all collector configurations", func(t *testing.T) {
		configs, err := collectors.BuildConfigurationOptions()
		require.Nil(t, err)
		require.Len(t, configs, 9)
		require.Contains(t, configs, "system_db")
		require.Contains(t, configs, "config")
		require.Contains(t, configs, "summary")
		require.Contains(t, configs, "system")
		require.Contains(t, configs, "logs")
		require.Contains(t, configs, "db_logs")
		require.Contains(t, configs, "file")
		require.Contains(t, configs, "command")
		require.Contains(t, configs, "zookeeper_db")
	})
}
