#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The evaluation time is 1700000000 (exactly representable as a Float) and all samples end at that instant,
# so the range window [3m] = (1699999820, 1700000000] captures the three most recent samples of each series.
# The ts_of_* functions return the Unix timestamp (in seconds) of the selected sample.

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
    ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'up', {'instance':'host2'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'up', {'instance':'host3'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000004', 'up', {'instance':'host4'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000005', 'up', {'instance':'host5'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000006', 'up', {'instance':'host6'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    -- host1: 10, 20, 30 (min at 1699999880, max at 1700000000)
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30),
    -- host2: 25, 15, 5 (values decreasing: min at 1700000000, max at 1699999880)
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999880, 3, 'UTC'), 25),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999940, 3, 'UTC'), 15),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 5),
    -- host3: constant 100 (min and max tie across all samples -> latest timestamp 1700000000 wins)
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999880, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999940, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 100),
    -- host4: only one sample, before the [3m] window, so it is absent from the range
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1699999000, 3, 'UTC'), 7),
    -- host5: nan, 10, 30 (a real value always beats NaN: min/max are 10 @1699999940 and 30 @1700000000)
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1699999880, 3, 'UTC'), nan),
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1699999940, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1700000000, 3, 'UTC'), 30),
    -- host6: nan, nan, nan (all-NaN window: min/max fall back to the latest timestamp, 1700000000)
    ('00000000-0000-0000-0000-000000000006', toDateTime64(1699999880, 3, 'UTC'), nan),
    ('00000000-0000-0000-0000-000000000006', toDateTime64(1699999940, 3, 'UTC'), nan),
    ('00000000-0000-0000-0000-000000000006', toDateTime64(1700000000, 3, 'UTC'), nan);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

# Series come back in an unspecified order, so multi-series outputs are piped through `sort`.
echo "-- ts_of_first_over_time(up[3m]): timestamp (Unix seconds) of the earliest sample; keeps the metric name."
echo "-- earliest sample of every series is at 1699999880; host4's only sample is older than the window, so it is dropped."
promql_client -q "ts_of_first_over_time(up[3m])" | sort

echo "-- ts_of_last_over_time(up[3m]): timestamp of the latest sample; keeps the metric name."
echo "-- latest sample of every series is at 1700000000."
promql_client -q "ts_of_last_over_time(up[3m])" | sort

echo "-- ts_of_min_over_time(up[3m]): timestamp of the minimum-value sample (latest one on ties); drops the metric name."
echo "-- host1 min 10 @1699999880, host2 min 5 @1700000000, host3 constant -> latest tie @1700000000."
echo "-- host5 min 10 (a NaN sample loses to any real value) @1699999940, host6 all-NaN -> latest timestamp @1700000000."
promql_client -q "ts_of_min_over_time(up[3m])" | sort

echo "-- ts_of_max_over_time(up[3m]): timestamp of the maximum-value sample (latest one on ties); drops the metric name."
echo "-- host1 max 30 @1700000000, host2 max 25 @1699999880, host3 constant -> latest tie @1700000000."
echo "-- host5 max 30 (a NaN sample loses to any real value) @1700000000, host6 all-NaN -> latest timestamp @1700000000."
promql_client -q "ts_of_max_over_time(up[3m])" | sort

echo "-- Float32 sample values: timestamps still come back as exact Float64 Unix seconds"
echo "-- (1699999940 and 1699999880 are not representable in Float32 and would round to multiples of 128)."
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts32_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float32) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts32_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts32_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts32 ENGINE = TimeSeries DATA ts32_data TAGS ts32_tags METRICS ts32_metrics;

INSERT INTO ts32_tags VALUES
    ('00000000-0000-0000-0000-000000000011', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));
INSERT INTO ts32_data VALUES
    ('00000000-0000-0000-0000-000000000011', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000011', toDateTime64(1699999940, 3, 'UTC'), 5),
    ('00000000-0000-0000-0000-000000000011', toDateTime64(1700000000, 3, 'UTC'), 20);
"

promql_client32()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts32 --promql_evaluation_time 1700000000 "$@"
}

promql_client32 -q "ts_of_first_over_time(up[3m])"
promql_client32 -q "ts_of_min_over_time(up[3m])"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts32"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts32_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts32_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts32_metrics"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
