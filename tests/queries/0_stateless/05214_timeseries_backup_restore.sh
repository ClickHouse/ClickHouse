#!/usr/bin/env bash
# BACKUP and RESTORE of a TimeSeries table of the current version: the definition and the data
# of all the inner tables (samples, recent samples, tags, metric families, histograms) must survive the round trip.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0"

# Prints the `create_table_query` of a table of the current database.
function get_create_query()
{
    $CLIENT --format TSVRaw -q "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

# Prints the `create_table_query` of a table of the current database with the table name replaced by a placeholder.
function get_create_query_without_name()
{
    get_create_query "$1" | sed 's/^CREATE TABLE [^ ]* /CREATE TABLE <table> /'
}

# Prints the number of tables of the current database whose name matches a LIKE pattern.
function count_tables_like()
{
    $CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '$1'"
}

# Prints the number of inner tables of each kind in the current database.
function count_inner_tables()
{
    count_tables_like '.inner\_id.samples.%'
    count_tables_like '.inner\_id.recentsamples.%'
    count_tables_like '.inner\_id.tags.%'
    count_tables_like '.inner\_id.metricfamilies.%'
    count_tables_like '.inner\_id.histograms.%'
}

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries"
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples, metric_family, type, unit, help) VALUES
    ('memory_usage_bytes', {'job': 'test', 'instance': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 10.), (toDateTime64('2026-01-01 00:00:15', 3), 12.)], 'memory_usage_bytes', 'gauge', 'bytes', 'Memory usage'),
    ('disk_usage_bytes', {'job': 'test', 'instance': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)], 'disk_usage_bytes', 'gauge', 'bytes', 'Disk usage'),
    ('up', {'job': 'test'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)], 'up', 'gauge', '', 'Whether the target is up')"

echo '--- the table has five inner tables ---'
count_inner_tables

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLIENT -q "BACKUP TABLE ts TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "RESTORE TABLE ts AS ts_restored FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"

echo '--- the restored table has the same definition ---'
get_create_query ts_restored | grep -c "version = [0-9]*"
diff <(get_create_query_without_name ts) <(get_create_query_without_name ts_restored) && echo 'definitions are equal'

echo '--- the restored table has its own inner tables ---'
count_inner_tables

echo '--- the restored table has the same data ---'
$CLIENT -q "SELECT * FROM ts ORDER BY ALL"
echo 'restored:'
$CLIENT -q "SELECT * FROM ts_restored ORDER BY ALL"

$CLIENT -q "DROP TABLE ts_restored"
$CLIENT -q "DROP TABLE ts"
