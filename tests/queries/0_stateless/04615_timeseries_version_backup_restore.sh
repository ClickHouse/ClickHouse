#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - Kept aligned with the other TimeSeries tests: the experimental TimeSeries table engine
#   does not round-trip through DatabaseReplicated.
#
# A TimeSeries table definition without the `version` setting means the initial version, so RESTORE
# must treat a definition stamped with the initial version and a definition without the setting as
# the same table definition. This emulates restoring a backup made before versioning was introduced
# into a table re-created by a server which stamps the version.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_backup_compat ENGINE = TimeSeries"

backup_name="Disk('backups', '${CLICKHOUSE_DATABASE}_ts_compat')"

echo '--- backup of a table stamped with the initial version ---'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE ts_backup_compat TO $backup_name" | cut -f2

echo '--- re-create the same table without the version in the stored definition (as if created by a pre-versioning server) ---'
create_query=$($CLICKHOUSE_CLIENT -q "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'ts_backup_compat'")
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_backup_compat SYNC"
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
attach_query=${create_query/CREATE TABLE ${CLICKHOUSE_DATABASE}.ts_backup_compat/ATTACH TABLE ts_backup_compat UUID \'$uuid\'}
attach_query=${attach_query/ SETTINGS version = 1/}
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "$attach_query"
$CLICKHOUSE_CLIENT -q "SELECT position(create_table_query, 'version') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'ts_backup_compat'"

echo '--- restoring the stamped backup into the unstamped table succeeds ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ts_backup_compat FROM $backup_name SETTINGS structure_only = true" | cut -f2

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_backup_compat"
