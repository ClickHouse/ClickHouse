#!/usr/bin/env bash
# A materialized view with `ENGINE = TimeSeries` created with `force_primary_key_reverse_order` must survive
# BACKUP and RESTORE: the inner tables of its hidden `TimeSeries` table are regenerated on restore, so they
# must have the same sorting keys as the parts attached from the backup, and key lookups must find all rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0 --force_primary_key_reverse_order=1"

function metric_families_table()
{
    $CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.metricfamilies.%'"
}

function sorting_keys()
{
    $CLIENT -q "SELECT engine, sorting_key FROM system.tables WHERE database = currentDatabase() AND engine LIKE '%MergeTree' ORDER BY ALL"
}

function lookup()
{
    local table
    table=$(metric_families_table)
    $CLIENT -q "SELECT count() FROM \`$table\` WHERE metric_family = 'm000042'"
    $CLIENT -q "SELECT countIf(metric_family = 'm000042') FROM \`$table\`"
}

$CLIENT -q "CREATE MATERIALIZED VIEW mv_ts ENGINE = TimeSeries AS SELECT 1 AS a"
$CLIENT -q "INSERT INTO \`$(metric_families_table)\` (metric_family) SELECT 'm' || leftPad(toString(number), 6, '0') FROM numbers(100000)"

sorting_keys > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_keys_before"
echo '--- before backup ---'
lookup

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLIENT -q "BACKUP TABLE mv_ts TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "DROP TABLE mv_ts SYNC"
$CLIENT -q "RESTORE TABLE mv_ts FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"

echo '--- after restore ---'
diff "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_keys_before" <(sorting_keys) && echo 'sorting keys are equal'
lookup

rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_keys_before"
$CLIENT -q "DROP TABLE mv_ts"
