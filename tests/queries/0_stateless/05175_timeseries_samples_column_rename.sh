#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: `ATTACH TABLE` with an explicit UUID is not allowed there.
#
# The outer column `time_series` of a TimeSeries table was renamed to `samples` in version 2 (see TimeSeriesVersion.h).
# The old name is still accepted in a CREATE query, and tables of older versions, which keep the old name
# in their stored definitions, must get the new name on ATTACH.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reference contains rendered DateTime64 values, so the timezone is fixed.
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"

echo '--- the old name in a new CREATE query is normalized to the new name and its type is kept ---'
$CLIENT -q "CREATE TABLE ts_old_name (time_series Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries"
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_old_name' AND name IN ('samples', 'time_series')"
$CLIENT -q "SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \\((.*?)\\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_old_name'"
$CLIENT -q "DROP TABLE ts_old_name"

echo '--- a table of version 1 declaring the old name gets the new name on ATTACH ---'
$CLIENT -q "CREATE TABLE ts_old_samples (id UUID, timestamp DateTime64(3), value Float32) ENGINE = MergeTree ORDER BY (id, timestamp)"
$CLIENT -q "CREATE TABLE ts_old_tags (id UUID, metric_name String, tags Map(String, String), min_time DateTime64(3), max_time DateTime64(3)) ENGINE = ReplacingMergeTree ORDER BY (metric_name, id)"
$CLIENT -q "CREATE TABLE ts_old_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name"

# The definition below is what a server of version 1 stores for a table with external targets.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_old UUID '$uuid' (metric_name String, tags Map(String, String), time_series Array(Tuple(DateTime64(3), Float32)), metric_family String, type String, unit String, help String) ENGINE = TimeSeries SAMPLES ts_old_samples TAGS ts_old_tags METRICS ts_old_metrics SETTINGS version = 1, recent_samples_ttl_seconds = 0"
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_old' ORDER BY position"
$CLIENT -q "SELECT extract(create_table_query, 'version = (\d+)'), position(create_table_query, 'time_series') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_old'"

echo '--- the attached table can be written and read ---'
$CLIENT -q "INSERT INTO ts_old (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3), 1), (toDateTime64(1060, 3), 2)])"
$CLIENT -q "SELECT metric_name, samples FROM ts_old"
$CLIENT -q "SELECT * FROM prometheusQueryRange(ts_old, 'up', 1000, 1060, 60) FORMAT TSVWithNamesAndTypes"

$CLIENT -q "DROP TABLE ts_old"
$CLIENT -q "DROP TABLE ts_old_samples"
$CLIENT -q "DROP TABLE ts_old_tags"
$CLIENT -q "DROP TABLE ts_old_metrics"
