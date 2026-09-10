#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ATTACH TABLE` with an explicit UUID is not allowed there, and it creates an Ordinary database.
#
# The "metrics" target of a TimeSeries table is named "metric families" since version 2 of the table engine:
# the keyword is `METRIC FAMILIES` (`METRICS` is kept as an alias), the table function is `timeSeriesMetricFamilies`
# (`timeSeriesMetrics` is kept as an alias), and the inner table is named `.inner_id.metricfamilies.<uuid>`.
# Tables of the older versions keep the inner table name `.inner_id.metrics.<uuid>`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0"

# Prints the `create_table_query` of a table of the current database, with the UUIDs of the table and of its inner tables.
function get_create_query()
{
    $CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 --format TSVRaw -q "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

# Prints the number of tables of the current database whose name matches a LIKE pattern.
function count_tables_like()
{
    $CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '$1'"
}

echo '--- a new table: the METRIC FAMILIES keyword, version 2, the .inner_id.metricfamilies inner table ---'
$CLIENT -q "CREATE TABLE ts_new ENGINE = TimeSeries METRIC FAMILIES INNER ENGINE = ReplacingMergeTree"
get_create_query ts_new | grep -o "METRIC FAMILIES INNER UUID\|METRIC FAMILIES INNER COLUMNS\|METRIC FAMILIES INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name\|version = [0-9]*"
count_tables_like '.inner\_id.metricfamilies.%'
count_tables_like '.inner\_id.metrics.%'

echo '--- the metric families are written and read through the inner table ---'
$CLIENT -q "INSERT INTO ts_new (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'job': 'test'}, [(now64(3), 1.)], 'http_requests_total', 'counter', 'requests', 'Total HTTP requests')"
$CLIENT -q "SELECT metric_family_name, type, unit, help FROM timeSeriesMetricFamilies(ts_new)"
$CLIENT -q "SELECT metric_family_name, type, unit, help FROM timeSeriesMetricFamilies(currentDatabase(), 'ts_new')"
echo 'the alias timeSeriesMetrics still works:'
$CLIENT -q "SELECT metric_family_name, type, unit, help FROM timeSeriesMetrics(ts_new)"
echo 'the outer columns are read from the metric families table:'
$CLIENT -q "SELECT DISTINCT metric_family, type, unit, help FROM ts_new"
$CLIENT -q "DROP TABLE ts_new"

echo '--- the METRICS keyword is an alias of METRIC FAMILIES ---'
$CLIENT -q "CREATE TABLE ts_alias_kw ENGINE = TimeSeries METRICS INNER ENGINE = ReplacingMergeTree"
get_create_query ts_alias_kw | grep -o "METRIC FAMILIES INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
get_create_query ts_alias_kw | grep -c "METRICS"
$CLIENT -q "DROP TABLE ts_alias_kw"

echo '--- an external metric families table can be specified with either keyword ---'
$CLIENT -q "CREATE TABLE mf_external (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
$CLIENT -q "CREATE TABLE ts_external ENGINE = TimeSeries METRIC FAMILIES mf_external"
get_create_query ts_external | grep -o "METRIC FAMILIES [a-z0-9_.]*" | sed "s/${CLICKHOUSE_DATABASE}\./<db>./"
$CLIENT -q "DROP TABLE ts_external"
$CLIENT -q "CREATE TABLE ts_external ENGINE = TimeSeries METRICS mf_external"
get_create_query ts_external | grep -o "METRIC FAMILIES [a-z0-9_.]*" | sed "s/${CLICKHOUSE_DATABASE}\./<db>./"
$CLIENT -q "INSERT INTO ts_external (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'Whether the target is up')"
$CLIENT -q "SELECT * FROM mf_external"
$CLIENT -q "DROP TABLE ts_external"
$CLIENT -q "DROP TABLE mf_external"

echo '--- the old kind name in the AST JSON is understood ---'
$CLIENT -q "SELECT formatQueryFromJSON(replaceAll(parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries METRIC FAMILIES db.m'), '\"MetricFamilies\"', '\"Metrics\"'))"

echo '--- a table of version 1 keeps the .inner_id.metrics inner table ---'
$CLIENT -q "CREATE TABLE ts_v1 ENGINE = TimeSeries SETTINGS version = 1"
get_create_query ts_v1 | grep -o "METRIC FAMILIES INNER UUID\|version = [0-9]*"
count_tables_like '.inner\_id.metrics.%'
count_tables_like '.inner\_id.metricfamilies.%'
$CLIENT -q "INSERT INTO ts_v1 (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'job': 'test'}, [(now64(3), 1.)], 'http_requests_total', 'counter', 'requests', 'Total HTTP requests')"
$CLIENT -q "SELECT metric_family_name, type FROM timeSeriesMetricFamilies(ts_v1)"

echo '--- a table of version 1 can be backed up and restored ---'
BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_v1"
$CLIENT -q "BACKUP TABLE ts_v1 TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "RESTORE TABLE ts_v1 AS ts_v1_restored FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"
get_create_query ts_v1_restored | grep -o "version = [0-9]*"
count_tables_like '.inner\_id.metrics.%'
$CLIENT -q "SELECT metric_family_name, type FROM timeSeriesMetricFamilies(ts_v1_restored)"
$CLIENT -q "DROP TABLE ts_v1_restored"
$CLIENT -q "DROP TABLE ts_v1"
count_tables_like '.inner\_id.%'

echo '--- a definition written by an older server (the METRICS keyword, version 1) is attached and works ---'
$CLIENT -q "CREATE TABLE ts_template ENGINE = TimeSeries SETTINGS version = 1, recent_samples_ttl_seconds = 0"
$CLIENT -q "INSERT INTO ts_template (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'job': 'test'}, [(now64(3), 1.)], 'http_requests_total', 'counter', 'requests', 'Total HTTP requests')"
# The definition of the old table is taken from a detached table of version 1 with the inner tables of that table,
# it's written back the way an older server would write it: with the METRICS keyword.
uuid=$($CLIENT -q "SELECT generateUUIDv4()")
attach_query=$(get_create_query ts_template | sed -e "s/^CREATE TABLE [^ ]* UUID '[^']*' /ATTACH TABLE ts_old UUID '$uuid' /" -e "s/METRIC FAMILIES/METRICS/g")
$CLIENT -q "DETACH TABLE ts_template"
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLIENT --send_logs_level=fatal -q "$attach_query"
get_create_query ts_old | grep -o "METRIC FAMILIES INNER UUID\|METRIC FAMILIES INNER COLUMNS\|METRIC FAMILIES INNER ENGINE\|version = [0-9]*"
get_create_query ts_old | grep -c "METRICS"
count_tables_like '.inner\_id.metrics.%'
$CLIENT -q "SELECT metric_family_name, type FROM timeSeriesMetricFamilies(ts_old)"
$CLIENT -q "INSERT INTO ts_old (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('up', {'job': 'test'}, [(now64(3), 1.)], 'up', 'gauge', '', 'Whether the target is up')"
$CLIENT -q "SELECT metric_family_name, type FROM timeSeriesMetricFamilies(ts_old) ORDER BY metric_family_name"
$CLIENT -q "SELECT DISTINCT metric_family, type FROM ts_old ORDER BY metric_family"
echo 'dropping the attached table drops the inner tables:'
$CLIENT -q "DROP TABLE ts_old"
count_tables_like '.inner\_id.%'
$CLIENT -q "ATTACH TABLE ts_template"
$CLIENT -q "DROP TABLE ts_template"

echo '--- in an Ordinary database the inner tables are found by name, which depends on the version ---'
ORD="${CLICKHOUSE_DATABASE}_ord"
$CLIENT -q "DROP DATABASE IF EXISTS ${ORD} SYNC"
$CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"
$CLIENT -q "CREATE TABLE ${ORD}.ts_v1 ENGINE = TimeSeries SETTINGS version = 1"
$CLIENT -q "CREATE TABLE ${ORD}.ts_v2 ENGINE = TimeSeries"
$CLIENT -q "SELECT name FROM system.tables WHERE database = '${ORD}' AND name LIKE '.inner.metric%' ORDER BY name"
$CLIENT -q "INSERT INTO ${ORD}.ts_v1 (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'v1')"
$CLIENT -q "INSERT INTO ${ORD}.ts_v2 (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'v2')"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_v1)"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_v2)"
echo 'the inner tables are renamed together with the table and keep the version-specific name:'
$CLIENT -q "RENAME TABLE ${ORD}.ts_v1 TO ${ORD}.ts_v1_renamed, ${ORD}.ts_v2 TO ${ORD}.ts_v2_renamed"
$CLIENT -q "SELECT name FROM system.tables WHERE database = '${ORD}' AND name LIKE '.inner.metric%' ORDER BY name"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_v1_renamed)"
$CLIENT -q "SELECT help FROM timeSeriesMetricFamilies(${ORD}.ts_v2_renamed)"
$CLIENT -q "DROP TABLE ${ORD}.ts_v1_renamed"
$CLIENT -q "DROP TABLE ${ORD}.ts_v2_renamed"
$CLIENT -q "SELECT count() FROM system.tables WHERE database = '${ORD}'"
$CLIENT -q "DROP DATABASE ${ORD} SYNC"
