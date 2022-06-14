package system_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/system"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSystemConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for system collector", func(t *testing.T) {
		systemCollector := system.NewSystemCollector(&platform.ResourceManager{})
		conf := systemCollector.Configuration()
		require.Len(t, conf.Params, 0)
		require.Equal(t, []config.ConfigParam{}, conf.Params)
	})
}

func TestSystemCollect(t *testing.T) {
	t.Run("test default system collection", func(t *testing.T) {
		systemCollector := system.NewSystemCollector(&platform.ResourceManager{})
		diagSet, err := systemCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 7)
		require.Contains(t, diagSet.Frames, "disks")
		require.Contains(t, diagSet.Frames, "disk_usage")
		require.Contains(t, diagSet.Frames, "memory")
		require.Contains(t, diagSet.Frames, "memory_usage")
		require.Contains(t, diagSet.Frames, "cpu")
		require.Contains(t, diagSet.Frames, "processes")
		require.Contains(t, diagSet.Frames, "os")
		// responses here will vary depending on platform - mocking seems excessive so we test we have some data
		// disks
		require.Equal(t, []string{"name", "size", "physicalBlockSize", "driveType", "controller", "vendor", "model", "partitionName", "partitionSize", "mountPoint", "readOnly"}, diagSet.Frames["disks"].Columns())
		diskFrames, err := countFrameRows(diagSet, "disks")
		require.Greater(t, diskFrames, 0)
		require.Nil(t, err)
		// disk usage
		require.Equal(t, []string{"filesystem", "size", "used", "avail", "use%", "mounted on"}, diagSet.Frames["disk_usage"].Columns())
		diskUsageFrames, err := countFrameRows(diagSet, "disk_usage")
		require.Greater(t, diskUsageFrames, 0)
		require.Nil(t, err)
		// memory
		require.Equal(t, []string{"totalPhysical", "totalUsable", "supportedPageSizes"}, diagSet.Frames["memory"].Columns())
		memoryFrames, err := countFrameRows(diagSet, "memory")
		require.Greater(t, memoryFrames, 0)
		require.Nil(t, err)
		// memory_usage
		require.Equal(t, []string{"type", "total", "used", "free"}, diagSet.Frames["memory_usage"].Columns())
		memoryUsageFrames, err := countFrameRows(diagSet, "memory_usage")
		require.Greater(t, memoryUsageFrames, 0)
		require.Nil(t, err)
		// cpu
		require.Equal(t, []string{"processor", "vendor", "model", "core", "numThreads", "logical", "capabilities"}, diagSet.Frames["cpu"].Columns())
		cpuFrames, err := countFrameRows(diagSet, "cpu")
		require.Greater(t, cpuFrames, 0)
		require.Nil(t, err)
		// processes
		require.Equal(t, []string{"pid", "ppid", "stime", "time", "rss", "size", "faults", "minorFaults", "majorFaults", "user", "state", "priority", "nice", "command"}, diagSet.Frames["processes"].Columns())
		processesFrames, err := countFrameRows(diagSet, "processes")
		require.Greater(t, processesFrames, 0)
		require.Nil(t, err)
		// os
		require.Equal(t, []string{"hostname", "os", "goOs", "cpus", "core", "kernel", "platform"}, diagSet.Frames["os"].Columns())
		osFrames, err := countFrameRows(diagSet, "os")
		require.Greater(t, osFrames, 0)
		require.Nil(t, err)
	})
}

func countFrameRows(diagSet *data.DiagnosticBundle, frameName string) (int, error) {
	frame := diagSet.Frames[frameName]
	i := 0
	for {
		_, ok, err := frame.Next()
		if !ok {
			return i, err
		}
		if err != nil {
			return i, err
		}
		i++
	}
}
