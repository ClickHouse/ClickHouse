//go:build !no_docker

package platform_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// create a ClickHouse container
func createClickHouseContainer(t *testing.T, ctx context.Context) (testcontainers.Container, nat.Port) {
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
		Mounts: testcontainers.ContainerMounts{
			{
				Source: testcontainers.GenericBindMountSource{
					HostPath: path.Join(cwd, "../../testdata/docker/custom.xml"),
				},
				Target: "/etc/clickhouse-server/config.d/custom.xml",
			},
			{
				Source: testcontainers.GenericBindMountSource{
					HostPath: path.Join(cwd, "../../testdata/docker/admin.xml"),
				},
				Target: "/etc/clickhouse-server/users.d/admin.xml",
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

	p, err := clickhouseContainer.MappedPort(ctx, "9000")
	if err != nil {
		// can't test without a port
		panic(err)
	}

	return clickhouseContainer, p
}

func TestConnect(t *testing.T) {
	t.Run("can only connect once", func(t *testing.T) {
		ctx := context.Background()

		clickhouseContainer, mappedPort := createClickHouseContainer(t, ctx)
		defer clickhouseContainer.Terminate(ctx) //nolint

		t.Setenv("CLICKHOUSE_DB_PORT", mappedPort.Port())

		port := mappedPort.Int()

		// get before connection
		manager := platform.GetResourceManager()
		require.Nil(t, manager.DbClient)
		// init connection
		err := manager.Connect("localhost", uint16(port), "", "")
		require.Nil(t, err)
		require.NotNil(t, manager.DbClient)
		// try and re-fetch connection
		err = manager.Connect("localhost", uint16(port), "", "")
		require.NotNil(t, err)
		require.Equal(t, "connect can only be called once", err.Error())
	})

}

func TestGetResourceManager(t *testing.T) {
	t.Run("get resource manager", func(t *testing.T) {
		manager := platform.GetResourceManager()
		require.NotNil(t, manager)
		manager2 := platform.GetResourceManager()
		require.NotNil(t, manager2)
		require.Equal(t, &manager, &manager2)
	})

}
