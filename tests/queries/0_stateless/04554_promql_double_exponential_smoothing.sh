#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The evaluation time is 1700000000 and all samples end at that instant, so the range window [3m] =
# (1699999820, 1700000000] captures the three most recent samples of each series. double_exponential_smoothing
# (Holt-Winters double exponential smoothing) returns the last smoothed value; it drops the metric name and
# needs at least two samples within the window.

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts ENGINE = TimeSeries DATA ts_data TAGS ts_tags METRICS ts_metrics;

INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'm', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'm', {'instance':'host2'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'm', {'instance':'host3'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000004', 'm', {'instance':'host4'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    -- host1: 10, 20, 30 (linear -> smoothed value is the last value 30)
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30),
    -- host2: 25, 15, 5 (linear decreasing -> smoothed value is the last value 5)
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999880, 3, 'UTC'), 25),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999940, 3, 'UTC'), 15),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 5),
    -- host3: 10, 20, 15 (non-linear)
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 15),
    -- host4: only one sample within the window, so it is dropped (needs at least two)
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 7);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

# Series come back in an unspecified order, so multi-series outputs are piped through `sort`.
echo "-- double_exponential_smoothing(m[3m], 0.5, 0.5): drops the metric name; host4 has one sample and is dropped."
echo "-- host1 (10,20,30) -> 30, host2 (25,15,5) -> 5, host3 (10,20,15) -> 22.5."
promql_client -q "double_exponential_smoothing(m[3m], 0.5, 0.5)" | sort

echo "-- double_exponential_smoothing(m[3m], 0.8, 0.3): different factors."
echo "-- host1 -> 30, host2 -> 5, host3 (10,20,15) -> 18."
promql_client -q "double_exponential_smoothing(m[3m], 0.8, 0.3)" | sort

echo "-- Invalid factors are rejected (must be in the open interval (0, 1))."
promql_client -q "double_exponential_smoothing(m[3m], 1.5, 0.5)" 2>&1 | grep -o "expects smoothing factor in the open interval (0, 1)" | head -1
promql_client -q "double_exponential_smoothing(m[3m], 0.5, 0)" 2>&1 | grep -o "expects trend factor in the open interval (0, 1)" | head -1

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
