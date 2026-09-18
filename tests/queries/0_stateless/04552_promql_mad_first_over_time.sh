#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The evaluation time is 1700000000 (exactly representable as a Float) and all samples end at that instant,
# so the range window [3m] = (1699999820, 1700000000] captures the three most recent samples of each series.

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
    ('00000000-0000-0000-0000-000000000005', 'up', {'instance':'host5'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    -- host1: 10, 20, 30 (median 20; MAD 10)
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30),
    -- host2: 5, 15, 25 (median 15; MAD 10)
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999880, 3, 'UTC'), 5),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999940, 3, 'UTC'), 15),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 25),
    -- host3: constant 100 (median 100; MAD 0)
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999880, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999940, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 100),
    -- host4: only one sample, before the [3m] window, so it is absent from the range
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1699999000, 3, 'UTC'), 7),
    -- host5: 10, nan, 30 (a NaN sample makes mad_over_time nan; first_over_time is unaffected since it just picks the earliest sample)
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1699999940, 3, 'UTC'), nan),
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1700000000, 3, 'UTC'), 30);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

# Series come back in an unspecified order, so multi-series outputs are piped through `sort`.
echo "-- first_over_time(up[3m]): the earliest (smallest timestamp) sample in the range window, keeps the metric name."
echo "-- host1 -> 10, host2 -> 5, host3 -> 100, host5 -> 10; host4's only sample is older than the window, so it is dropped."
promql_client -q "first_over_time(up[3m])" | sort

echo "-- mad_over_time(up[3m]): the median absolute deviation of the in-range samples, drops the metric name."
echo "-- host1 = 10 (median 20, deviations 10/0/10), host2 = 10 (median 15, deviations 10/0/10), host3 = 0, host5 = nan (a sample is NaN)."
promql_client -q "mad_over_time(up[3m])" | sort

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
