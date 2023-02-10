//go:build !no_docker

package utils_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func getProcessesInContainer(t *testing.T, container testcontainers.Container) ([]string, error) {
	result, reader, err := container.Exec(context.Background(), []string{"ps", "-aux"})
	if err != nil {
		return nil, err
	}
	require.Zero(t, result)
	require.NotNil(t, reader)

	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	require.NotNil(t, b)

	lines := strings.Split(string(b), "\n")

	// discard PS header
	return lines[1:], nil
}

func TestFindClickHouseProcessesAndConfigs(t *testing.T) {

	t.Run("can find ClickHouse processes and configs", func(t *testing.T) {
		// create a ClickHouse container
		ctx := context.Background()
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Println("unable to read current directory", err)
			os.Exit(1)
		}

		// run a ClickHouse container that guarantees that it runs only for the duration of the test
		req := testcontainers.ContainerRequest{
			Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", test.GetClickHouseTestVersion()),
			ExposedPorts: []string{"9000/tcp"},
			WaitingFor:   wait.ForLog("Ready for connections"),
			Mounts: testcontainers.ContainerMounts{
				{
					Source: testcontainers.GenericBindMountSource{
						HostPath: path.Join(cwd, "../../../testdata/docker/custom.xml"),
					},
					Target: "/etc/clickhouse-server/config.d/custom.xml",
				},
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

		t.Setenv("CLICKHOUSE_DB_PORT", p.Port())

		defer clickhouseContainer.Terminate(ctx) //nolint

		lines, err := getProcessesInContainer(t, clickhouseContainer)
		require.Nil(t, err)
		require.NotEmpty(t, lines)

		for _, line := range lines {
			parts := strings.Fields(line)
			if len(parts) < 11 {
				continue
			}
			if !strings.Contains(parts[10], "clickhouse-server") {
				continue
			}

			require.Equal(t, "/usr/bin/clickhouse-server", parts[10])
			require.Equal(t, "--config-file=/etc/clickhouse-server/config.xml", parts[11])
		}
	})
}
