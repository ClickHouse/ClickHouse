//go:build !no_docker

package utils_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	// create a ClickHouse container
	ctx := context.Background()
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("unable to read current directory", err)
		os.Exit(1)
	}
	// for now, we test against a hardcoded database-server version but we should make this a property
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", test.GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		BindMounts: map[string]string{
			"/etc/clickhouse-server/config.d/custom.xml": path.Join(cwd, "../../../testdata/docker/custom.xml"),
		},
	}
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// can't test without container
		panic(err)
	}

	p, _ := clickhouseContainer.MappedPort(ctx, "9000")

	os.Setenv("CLICKHOUSE_DB_PORT", p.Port())

	defer clickhouseContainer.Terminate(ctx) //nolint
	os.Exit(m.Run())
}

func TestFindClickHouseProcesses(t *testing.T) {

	t.Run("can find ClickHouse processes", func(t *testing.T) {
		processes, err := utils.FindClickHouseProcesses()
		require.Nil(t, err)
		// we might have clickhouse running locally during development as well as the above container so we allow 1 or more
		require.GreaterOrEqual(t, len(processes), 1)
		require.Equal(t, processes[0].List[0], "/usr/bin/clickhouse-server")
		// flexible as services/containers pass the config differently
		require.Contains(t, processes[0].List[1], "/etc/clickhouse-server/config.xml")
	})
}

func TestFindConfigsFromClickHouseProcesses(t *testing.T) {

	t.Run("can find ClickHouse configs", func(t *testing.T) {
		configs, err := utils.FindConfigsFromClickHouseProcesses()
		require.Nil(t, err)
		require.GreaterOrEqual(t, len(configs), 1)
		require.Equal(t, configs[0], "/etc/clickhouse-server/config.xml")
	})
}
