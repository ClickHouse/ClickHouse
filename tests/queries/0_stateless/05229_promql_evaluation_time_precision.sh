#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two samples ten seconds apart. A 32-bit float cannot hold 1700000100: it rounds to 1700000128,
# which is past the second sample, so an evaluation time held in one selects the wrong value.
# Use the generated metric-families target: this test does not depend on its versioned schema.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts ENGINE = TimeSeries DATA ts_data TAGS ts_tags;
INSERT INTO ts_tags VALUES ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1700000100, 3, 'UTC'), toDateTime64(1700000110, 3, 'UTC'));
INSERT INTO ts_data VALUES ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000100, 3, 'UTC'), 100), ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000110, 3, 'UTC'), 999);
"

echo "-- the setting holds the value it is given, over the native protocol"
$CLICKHOUSE_CLIENT -q "SET promql_evaluation_time = 1700000100; SELECT getSetting('promql_evaluation_time')"

echo "-- and over the command line, which used to round it"
$CLICKHOUSE_CLIENT --promql_evaluation_time 1700000100 -q "SELECT getSetting('promql_evaluation_time')"

echo "-- ground truth: the table function evaluates at the requested instant"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q \
    "SELECT timestamp, value FROM prometheusQuery(ts, 'up', 1700000100)"

echo "-- the dialect agrees with it instead of landing on the later sample"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql \
    --promql_table ts --promql_evaluation_time 1700000100 -q "up"

echo "-- a fractional instant survives too"
$CLICKHOUSE_CLIENT -q "SET promql_evaluation_time = 1700000100.5; SELECT getSetting('promql_evaluation_time')"
