#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique per run, so a previous run cannot leave an archive of this name behind.
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}.tar"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t0 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1 SYNC"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY c1 PARTITION BY (c1 % 10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE t0 (c1) SELECT number FROM numbers(500)"

$CLICKHOUSE_CLIENT --query "BACKUP TABLE t0 TO Disk('backups', '${backup_name}') FORMAT Null"

$CLICKHOUSE_CLIENT --query "RESTORE TABLE t0 AS t1 FROM Disk('backups', '${backup_name}') FORMAT Null"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t1 ORDER BY c1 LIMIT 10"

# Clean up.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t0 SYNC"
