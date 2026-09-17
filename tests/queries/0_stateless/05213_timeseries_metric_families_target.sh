#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the test creates an Ordinary database.
#
# The "metrics" target of a TimeSeries table is named "metric families" since version 4 of the table engine:
# the keyword is `METRIC FAMILIES` (`METRICS` is kept as an alias), the table function is `timeSeriesMetricFamilies`
# (`timeSeriesMetrics` is kept as an alias), and the inner table is named `.inner_id.metricfamilies.<uuid>`.
# Tables of the older versions keep the inner table name `.inner_id.metrics.<uuid>` and are written with the `METRICS` keyword.
# The keywords in the definition are covered by the unit test gtest_normalize_time_series_definition.cpp.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0"

# Prints the `create_table_query` of a table of the current database.
function get_create_query()
{
    $CLIENT --format TSVRaw -q "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

# Prints the number of tables of the current database whose name matches a LIKE pattern.
function count_tables_like()
{
    $CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '$1'"
}

echo '--- a new table: the METRIC FAMILIES keyword, the .inner_id.metricfamilies inner table ---'
$CLIENT -q "CREATE TABLE ts_new ENGINE = TimeSeries METRIC FAMILIES INNER ENGINE = ReplacingMergeTree"
get_create_query ts_new | grep -o "METRIC FAMILIES INNER COLUMNS\|METRIC FAMILIES INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
count_tables_like '.inner\_id.metricfamilies.%'
count_tables_like '.inner\_id.metrics.%'
$CLIENT -q "INSERT INTO ts_new (metric_name, tags, samples, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'job': 'test'}, [(now64(3), 1.)], 'http_requests_total', 'counter', 'requests', 'Total HTTP requests')"
$CLIENT -q "SELECT metric_family_name, type, unit, help FROM timeSeriesMetricFamilies(ts_new)"
echo 'the alias timeSeriesMetrics still works:'
$CLIENT -q "SELECT metric_family_name, type, unit, help FROM timeSeriesMetrics(ts_new)"
$CLIENT -q "DROP TABLE ts_new"

echo '--- the old kind name in the AST JSON is understood ---'
$CLIENT -q "SELECT formatQueryFromJSON(replaceAll(parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries METRIC FAMILIES db.m'), '\"MetricFamilies\"', '\"Metrics\"'))"
echo 'the AST JSON of a table of an older version uses the old kind name:'
$CLIENT -q "SELECT parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries SETTINGS version = 3 METRIC FAMILIES db.m') LIKE '%\"kind\":\"Metrics\"%'"
$CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries SETTINGS version = 3 METRIC FAMILIES db.m'))"

echo '--- a table of version 3 keeps the .inner_id.metrics inner table and the METRICS keyword ---'
$CLIENT -q "CREATE TABLE ts_v3 ENGINE = TimeSeries SETTINGS version = 3 METRIC FAMILIES INNER ENGINE = ReplacingMergeTree"
get_create_query ts_v3 | grep -o "METRIC FAMILIES\|METRICS INNER COLUMNS\|METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name\|version = [0-9]*"
count_tables_like '.inner\_id.metrics.%'
count_tables_like '.inner\_id.metricfamilies.%'

echo '--- a table of version 3 can be backed up and restored ---'
$CLIENT -q "INSERT INTO ts_v3 (metric_name, tags, samples, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'job': 'test'}, [(now64(3), 1.)], 'http_requests_total', 'counter', 'requests', 'Total HTTP requests')"
BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_v3"
$CLIENT -q "BACKUP TABLE ts_v3 TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "RESTORE TABLE ts_v3 AS ts_v3_restored FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"
get_create_query ts_v3_restored | grep -o "METRIC FAMILIES\|METRICS INNER COLUMNS\|version = [0-9]*"
count_tables_like '.inner\_id.metrics.%'
$CLIENT -q "SELECT metric_family_name, type FROM timeSeriesMetricFamilies(ts_v3_restored)"
$CLIENT -q "DROP TABLE ts_v3_restored"
$CLIENT -q "DROP TABLE ts_v3"
count_tables_like '.inner\_id.%'

echo '--- in an Ordinary database the inner tables are found by name, which depends on the version ---'
ORD="${CLICKHOUSE_DATABASE}_ord"
$CLIENT -q "DROP DATABASE IF EXISTS ${ORD} SYNC"
$CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"
$CLIENT -q "CREATE TABLE ${ORD}.ts_v3 ENGINE = TimeSeries SETTINGS version = 3"
$CLIENT -q "CREATE TABLE ${ORD}.ts_latest ENGINE = TimeSeries"
$CLIENT -q "SELECT name FROM system.tables WHERE database = '${ORD}' AND name LIKE '.inner.metric%' ORDER BY name"
$CLIENT -q "INSERT INTO ${ORD}.ts_v3 (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'v3')"
$CLIENT -q "INSERT INTO ${ORD}.ts_latest (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'latest')"
echo 'the inner tables are renamed together with the table and keep the version-specific name:'
$CLIENT -q "RENAME TABLE ${ORD}.ts_v3 TO ${ORD}.ts_v3_renamed, ${ORD}.ts_latest TO ${ORD}.ts_latest_renamed"
$CLIENT -q "SELECT name FROM system.tables WHERE database = '${ORD}' AND name LIKE '.inner.metric%' ORDER BY name"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_v3_renamed)"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_latest_renamed)"
$CLIENT -q "DROP TABLE ${ORD}.ts_v3_renamed"
$CLIENT -q "DROP TABLE ${ORD}.ts_latest_renamed"
$CLIENT -q "SELECT count() FROM system.tables WHERE database = '${ORD}'"
$CLIENT -q "DROP DATABASE ${ORD} SYNC"
