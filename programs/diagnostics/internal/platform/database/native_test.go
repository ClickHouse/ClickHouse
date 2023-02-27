//go:build !no_docker

package database_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/database"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	// create a ClickHouse container
	ctx := context.Background()
	cwd, err := os.Getwd()
	if err != nil {
		// can't test without container
		panic(err)
	}

	// for now, we test against a hardcoded database-server version but we should make this a property
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", test.GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		BindMounts: map[string]string{
			"/etc/clickhouse-server/config.d/custom.xml": path.Join(cwd, "../../../testdata/docker/custom.xml"),
			"/etc/clickhouse-server/users.d/admin.xml":   path.Join(cwd, "../../../testdata/docker/admin.xml"),
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

func getClient(t *testing.T) *database.ClickhouseNativeClient {
	mappedPort, err := strconv.Atoi(os.Getenv("CLICKHOUSE_DB_PORT"))
	if err != nil {
		t.Fatal("Unable to read port value from environment")
	}
	clickhouseClient, err := database.NewNativeClient("localhost", uint16(mappedPort), "", "")
	if err != nil {
		t.Fatalf("unable to build client : %v", err)
	}
	return clickhouseClient
}

func TestReadTableNamesForDatabase(t *testing.T) {
	clickhouseClient := getClient(t)
	t.Run("client can read tables for a database", func(t *testing.T) {
		tables, err := clickhouseClient.ReadTableNamesForDatabase("system")
		require.Nil(t, err)
		require.GreaterOrEqual(t, len(tables), 70)
		require.Contains(t, tables, "merge_tree_settings")
	})
}

func TestReadTable(t *testing.T) {
	clickhouseClient := getClient(t)
	t.Run("client can get all rows for system.disks table", func(t *testing.T) {
		// we read the table system.disks as this should contain only 1 row
		frame, err := clickhouseClient.ReadTable("system", "disks", []string{}, data.OrderBy{}, 10)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [7]string{"name", "path", "free_space", "total_space", "keep_free_space", "type", "cache_path"})
		i := 0
		for {
			values, ok, err := frame.Next()
			if i == 0 {
				require.Nil(t, err)
				require.True(t, ok)
				require.Equal(t, "default", values[0])
				require.Equal(t, "/var/lib/clickhouse/", values[1])
				require.Greater(t, values[2], uint64(0))
				require.Greater(t, values[3], uint64(0))
				require.Equal(t, values[4], uint64(0))
				require.Equal(t, "local", values[5])
			} else {
				require.False(t, ok)
				break
			}
			i += 1
		}
	})

	t.Run("client can get all rows for system.databases table", func(t *testing.T) {
		// we read the table system.databases as this should be small and consistent on fresh db instances
		frame, err := clickhouseClient.ReadTable("system", "databases", []string{}, data.OrderBy{}, 10)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [6]string{"name", "engine", "data_path", "metadata_path", "uuid", "comment"})
		expectedRows := [4][3]string{{"INFORMATION_SCHEMA", "Memory", "/var/lib/clickhouse/"},
			{"default", "Atomic", "/var/lib/clickhouse/store/"},
			{"information_schema", "Memory", "/var/lib/clickhouse/"},
			{"system", "Atomic", "/var/lib/clickhouse/store/"}}
		i := 0
		for {
			values, ok, err := frame.Next()

			if i < 4 {
				require.Nil(t, err)
				require.True(t, ok)
				require.Equal(t, expectedRows[i][0], values[0])
				require.Equal(t, expectedRows[i][1], values[1])
				require.Equal(t, expectedRows[i][2], values[2])
				require.NotNil(t, values[3])
				require.NotNil(t, values[4])
				require.Equal(t, "", values[5])
			} else {
				require.False(t, ok)
				break
			}
			i += 1
		}
	})

	t.Run("client can get all rows for system.databases table with except", func(t *testing.T) {
		frame, err := clickhouseClient.ReadTable("system", "databases", []string{"data_path", "comment"}, data.OrderBy{}, 10)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [4]string{"name", "engine", "metadata_path", "uuid"})
	})

	t.Run("client can limit rows for system.databases", func(t *testing.T) {
		frame, err := clickhouseClient.ReadTable("system", "databases", []string{}, data.OrderBy{}, 1)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [6]string{"name", "engine", "data_path", "metadata_path", "uuid", "comment"})
		expectedRows := [1][3]string{{"INFORMATION_SCHEMA", "Memory", "/var/lib/clickhouse/"}}
		i := 0
		for {
			values, ok, err := frame.Next()
			if i == 0 {
				require.Nil(t, err)
				require.True(t, ok)
				require.Equal(t, expectedRows[i][0], values[0])
				require.Equal(t, expectedRows[i][1], values[1])
				require.Equal(t, expectedRows[i][2], values[2])
				require.NotNil(t, values[3])
				require.NotNil(t, values[4])
				require.Equal(t, "", values[5])
			} else {
				require.False(t, ok)
				break
			}
			i += 1
		}
	})

	t.Run("client can order rows for system.databases", func(t *testing.T) {
		frame, err := clickhouseClient.ReadTable("system", "databases", []string{}, data.OrderBy{
			Column: "engine",
			Order:  data.Asc,
		}, 10)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [6]string{"name", "engine", "data_path", "metadata_path", "uuid", "comment"})
		expectedRows := [4][3]string{
			{"default", "Atomic", "/var/lib/clickhouse/store/"},
			{"system", "Atomic", "/var/lib/clickhouse/store/"},
			{"INFORMATION_SCHEMA", "Memory", "/var/lib/clickhouse/"},
			{"information_schema", "Memory", "/var/lib/clickhouse/"},
		}
		i := 0
		for {
			values, ok, err := frame.Next()

			if i < 4 {
				require.Nil(t, err)
				require.True(t, ok)
				require.Equal(t, expectedRows[i][0], values[0])
				require.Equal(t, expectedRows[i][1], values[1])
				require.Equal(t, expectedRows[i][2], values[2])
				require.NotNil(t, values[3])
				require.NotNil(t, values[4])
				require.Equal(t, "", values[5])
			} else {
				require.False(t, ok)
				break
			}
			i += 1
		}
	})
}

func TestExecuteStatement(t *testing.T) {
	clickhouseClient := getClient(t)
	t.Run("client can execute any statement", func(t *testing.T) {
		statement := "SELECT path, count(*) as count FROM system.disks GROUP BY path;"
		frame, err := clickhouseClient.ExecuteStatement("engines", statement)
		require.Nil(t, err)
		require.ElementsMatch(t, frame.Columns(), [2]string{"path", "count"})
		expectedRows := [1][2]interface{}{
			{"/var/lib/clickhouse/", uint64(1)},
		}
		i := 0
		for {
			values, ok, err := frame.Next()
			if !ok {
				require.Nil(t, err)
				break
			}
			require.Nil(t, err)
			require.Equal(t, expectedRows[i][0], values[0])
			require.Equal(t, expectedRows[i][1], values[1])
			i++
		}
		fmt.Println(i)
	})
}

func TestVersion(t *testing.T) {
	clickhouseClient := getClient(t)
	t.Run("client can read version", func(t *testing.T) {
		version, err := clickhouseClient.Version()
		require.Nil(t, err)
		require.NotEmpty(t, version)
	})
}
