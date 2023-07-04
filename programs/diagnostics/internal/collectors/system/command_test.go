package system_test

import (
	"fmt"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/system"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
)

func TestCommandConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for file collector", func(t *testing.T) {
		commandCollector := system.NewCommandCollector(&platform.ResourceManager{})
		conf := commandCollector.Configuration()
		require.Len(t, conf.Params, 1)
		require.IsType(t, config.StringParam{}, conf.Params[0])
		command, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.True(t, command.Required())
		require.Equal(t, command.Name(), "command")
		require.Equal(t, "", command.Value)
	})
}

func TestCommandCollect(t *testing.T) {
	t.Run("test simple command with args", func(t *testing.T) {
		commandCollector := system.NewCommandCollector(&platform.ResourceManager{})
		bundle, err := commandCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "ls -l ../../../testdata",
					Param:      config.NewParam("command", "Command to execute", true),
					AllowEmpty: false,
				},
			},
		})
		require.Nil(t, err)
		require.Nil(t, bundle.Errors.Errors)
		require.Len(t, bundle.Frames, 1)
		require.Contains(t, bundle.Frames, "output")
		require.Equal(t, bundle.Frames["output"].Columns(), []string{"command", "stdout", "stderr", "error"})
		memFrame := bundle.Frames["output"].(data.MemoryFrame)
		values, ok, err := memFrame.Next()
		require.True(t, ok)
		require.Nil(t, err)
		fmt.Println(values)
		require.Len(t, values, 4)
		require.Equal(t, "ls -l ../../../testdata", values[0])
		require.Contains(t, values[1], "configs")
		require.Contains(t, values[1], "docker")
		require.Contains(t, values[1], "log")
		require.Equal(t, "", values[2])
		require.Equal(t, "", values[3])
		values, ok, err = memFrame.Next()
		require.False(t, ok)
		require.Nil(t, err)
		require.Nil(t, values)
	})

	t.Run("test empty command", func(t *testing.T) {
		commandCollector := system.NewCommandCollector(&platform.ResourceManager{})
		bundle, err := commandCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "",
					Param:      config.NewParam("command", "Command to execute", true),
					AllowEmpty: false,
				},
			},
		})
		require.Equal(t, "parameter command is invalid - command cannot be empty", err.Error())
		require.Equal(t, &data.DiagnosticBundle{}, bundle)
	})

	t.Run("test invalid command", func(t *testing.T) {
		commandCollector := system.NewCommandCollector(&platform.ResourceManager{})
		bundle, err := commandCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "ls --invalid ../../../testdata",
					Param:      config.NewParam("command", "Command to execute", true),
					AllowEmpty: false,
				},
			},
		})
		// commands may error with output - we still capture on stderr
		require.Nil(t, err)
		require.Len(t, bundle.Errors.Errors, 1)
		require.Equal(t, "Unable to execute command: exit status 2", bundle.Errors.Errors[0].Error())
		require.Len(t, bundle.Frames, 1)
		require.Contains(t, bundle.Frames, "output")
		require.Equal(t, bundle.Frames["output"].Columns(), []string{"command", "stdout", "stderr", "error"})
		memFrame := bundle.Frames["output"].(data.MemoryFrame)
		values, ok, err := memFrame.Next()
		require.True(t, ok)
		require.Nil(t, err)
		require.Len(t, values, 4)
		require.Equal(t, "ls --invalid ../../../testdata", values[0])
		require.Equal(t, "", values[1])
		// exact values here may vary on platform
		require.NotEmpty(t, values[2])
		require.NotEmpty(t, values[3])
	})
}
