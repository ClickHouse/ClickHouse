#!/usr/bin/env bash
# A table defined from a table function keeps no data of its own, so `BACKUP` and `RESTORE` store and recreate only its
# definition, without running the table function: they work while its source is missing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) AS file('${CLICKHOUSE_TEST_UNIQUE_NAME}_missing.csv', 'CSV');
"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t TO $backup" | grep -o "BACKUP_CREATED"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t FROM $backup" | grep -o "RESTORED"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 't'"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
