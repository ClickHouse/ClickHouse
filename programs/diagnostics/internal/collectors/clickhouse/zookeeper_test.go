package clickhouse_test

import (
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
)

func TestZookeeperConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for system zookeeper collector", func(t *testing.T) {
		client := test.NewFakeClickhouseClient(make(map[string][]string))
		zkCollector := clickhouse.NewZookeeperCollector(&platform.ResourceManager{
			DbClient: client,
		})
		conf := zkCollector.Configuration()
		require.Len(t, conf.Params, 3)
		// check first param
		require.IsType(t, config.StringParam{}, conf.Params[0])
		pathPattern, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.False(t, pathPattern.Required())
		require.Equal(t, pathPattern.Name(), "path_pattern")
		require.Equal(t, "/clickhouse/{task_queue}/**", pathPattern.Value)
		// check second param
		require.IsType(t, config.IntParam{}, conf.Params[1])
		maxDepth, ok := conf.Params[1].(config.IntParam)
		require.True(t, ok)
		require.False(t, maxDepth.Required())
		require.Equal(t, "max_depth", maxDepth.Name())
		require.Equal(t, int64(8), maxDepth.Value)
		// check third param
		require.IsType(t, config.IntParam{}, conf.Params[2])
		rowLimit, ok := conf.Params[2].(config.IntParam)
		require.True(t, ok)
		require.False(t, rowLimit.Required())
		require.Equal(t, "row_limit", rowLimit.Name())
		require.Equal(t, int64(10), rowLimit.Value)
	})
}

func TestZookeeperCollect(t *testing.T) {
	level1 := test.NewFakeDataFrame("level_1", []string{"name", "value", "czxid", "mzxid", "ctime", "mtime", "version", "cversion", "aversion", "ephemeralOwner", "dataLength", "numChildren", "pzxid", "path"},
		[][]interface{}{
			{"name", "value", "czxid", "mzxid", "ctime", "mtime", "version", "cversion", "aversion", "ephemeralOwner", "dataLength", "numChildren", "pzxid", "path"},
			{"task_queue", "", "4", "4", "2022-02-22 13:30:15", "2022-02-22 13:30:15", "0", "1", "0", "0", "0", "1", "5", "/clickhouse"},
			{"copytasks", "", "525608", "525608", "2022-03-09 13:47:39", "2022-03-09 13:47:39", "0", "7", "0", "0", "0", "7", "526100", "/clickhouse"},
		},
	)
	level2 := test.NewFakeDataFrame("level_2", []string{"name", "value", "czxid", "mzxid", "ctime", "mtime", "version", "cversion", "aversion", "ephemeralOwner", "dataLength", "numChildren", "pzxid", "path"},
		[][]interface{}{
			{"ddl", "", "5", "5", "2022-02-22 13:30:15", "2022-02-22 13:30:15", "0", "0", "0", "0", "0", "0", "5", "/clickhouse/task_queue"},
		},
	)
	level3 := test.NewFakeDataFrame("level_2", []string{"name", "value", "czxid", "mzxid", "ctime", "mtime", "version", "cversion", "aversion", "ephemeralOwner", "dataLength", "numChildren", "pzxid", "path"},
		[][]interface{}{},
	)
	dbTables := map[string][]string{
		clickhouse.SystemDatabase: {"zookeeper"},
	}
	client := test.NewFakeClickhouseClient(dbTables)

	client.QueryResponses["SELECT name FROM system.zookeeper WHERE path='/clickhouse' LIMIT 10"] = &level1
	// can't reuse the frame as the first frame will be iterated as part of the recursive zookeeper search performed by the collector
	cLevel1 := test.NewFakeDataFrame("level_1", level1.Columns(), level1.Rows)
	client.QueryResponses["SELECT * FROM system.zookeeper WHERE path='/clickhouse' LIMIT 10"] = &cLevel1
	client.QueryResponses["SELECT name FROM system.zookeeper WHERE path='/clickhouse/task_queue' LIMIT 10"] = &level2
	cLevel2 := test.NewFakeDataFrame("level_2", level2.Columns(), level2.Rows)
	client.QueryResponses["SELECT * FROM system.zookeeper WHERE path='/clickhouse/task_queue' LIMIT 10"] = &cLevel2
	client.QueryResponses["SELECT name FROM system.zookeeper WHERE path='/clickhouse/task_queue/ddl' LIMIT 10"] = &level3
	cLevel3 := test.NewFakeDataFrame("level_3", level3.Columns(), level3.Rows)
	client.QueryResponses["SELECT * FROM system.zookeeper WHERE path='/clickhouse/task_queue/ddl' LIMIT 10"] = &cLevel3

	zKCollector := clickhouse.NewZookeeperCollector(&platform.ResourceManager{
		DbClient: client,
	})

	t.Run("test default zookeeper collection", func(t *testing.T) {
		diagSet, err := zKCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 1)
		require.Contains(t, diagSet.Frames, "zookeeper_db")
		require.Equal(t, "clickhouse", diagSet.Frames["zookeeper_db"].Name())
		require.IsType(t, data.HierarchicalFrame{}, diagSet.Frames["zookeeper_db"])
		checkFrame(t, diagSet.Frames["zookeeper_db"], level1.Rows)
		require.Equal(t, level1.Columns(), diagSet.Frames["zookeeper_db"].Columns())
		hierarchicalFrame := diagSet.Frames["zookeeper_db"].(data.HierarchicalFrame)
		require.Len(t, hierarchicalFrame.SubFrames, 1)
		checkFrame(t, hierarchicalFrame.SubFrames[0], cLevel2.Rows)
		require.Equal(t, cLevel2.Columns(), hierarchicalFrame.SubFrames[0].Columns())
		hierarchicalFrame = hierarchicalFrame.SubFrames[0]
		require.Len(t, hierarchicalFrame.SubFrames, 1)
		checkFrame(t, hierarchicalFrame.SubFrames[0], cLevel3.Rows)
		require.Equal(t, cLevel3.Columns(), hierarchicalFrame.SubFrames[0].Columns())
	})
}
