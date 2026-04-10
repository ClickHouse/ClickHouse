#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backups_disk_root=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name='backups'" 2>/dev/null)
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}.tar"

# Clean up any leftover backup from a previous run.
rm -rf "${backups_disk_root:?}/${backup_name}" 2>/dev/null || true

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t0 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1 SYNC"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY c1 PARTITION BY (c1 % 6451)"
$CLICKHOUSE_CLIENT --query "SET min_insert_block_size_rows = 64, optimize_trivial_insert_select = 1; INSERT INTO TABLE t0 (c1) SELECT number FROM numbers(500)"

$CLICKHOUSE_CLIENT --query "BACKUP TABLE t0 TO Disk('backups', '${backup_name}') FORMAT Null"

$CLICKHOUSE_CLIENT --query "RESTORE TABLE t0 AS t1 FROM Disk('backups', '${backup_name}') FORMAT Null"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t1 ORDER BY c1 LIMIT 10"

# Clean up.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t1 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t0 SYNC"
rm -rf "${backups_disk_root:?}/${backup_name}" 2>/dev/null || true
