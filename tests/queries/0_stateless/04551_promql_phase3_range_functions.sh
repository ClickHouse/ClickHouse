#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE ON TIMESTAMPS:
# The evaluation time is passed via the `promql_evaluation_time` setting, whose type is Float.
# A large Unix timestamp such as 1700000120 is not exactly representable in the intermediate
# float used to carry the setting, so it would be rounded to the nearest representable instant.
# To keep the extrapolated values exact and easy to verify, this test evaluates at 1700000000
# (which is exactly representable) and places all samples so that they end at that instant.

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

-- Four series, sampled every 60s and ending at the evaluation time 1700000000:
--   timestamps = 1699999880, 1699999940, 1700000000.
-- host1 / host2: linear ramps (used to check quantile_over_time and predict_linear).
-- host3: constant (predict_linear slope == 0 sanity check).
-- host4: a single sample before the [3m] range window, so it is absent from the range.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'up', {'instance':'host2'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'up', {'instance':'host3'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000004', 'up', {'instance':'host4'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    -- host1: linear ramp 10 -> 20 -> 30 (slope +1/6 per second)
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30),
    -- host2: linear ramp 5 -> 15 -> 25 (slope +1/6 per second)
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999880, 3, 'UTC'), 5),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1699999940, 3, 'UTC'), 15),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 25),
    -- host3: constant 100
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999880, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1699999940, 3, 'UTC'), 100),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 100),
    -- host4: only one sample, before the [3m] window (1699999820, 1700000000]
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1699999000, 3, 'UTC'), 7),
    -- up2/host1: a second metric that differs from up/host1 only by __name__, for the multi-metric
    -- absent_over_time regression (the internal presence grid must not collapse them into duplicates)
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1700000000, 3, 'UTC'), 1);
"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000005', 'up2', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

# Series come back in an unspecified order, so multi-series outputs are piped through `sort`.
echo "-- present_over_time(up[3m]): 1 for every series with a sample in the range window."
echo "-- host1/host2/host3 have samples in (1699999820, 1700000000]; host4's only sample is older, so it is dropped."
promql_client -q "present_over_time(up[3m])" | sort

echo "-- absent_over_time(up[3m]): the metric has samples in the range, so the result is empty."
promql_client -q "absent_over_time(up[3m])"

echo "-- absent_over_time(nonexistent_metric[5m]): no samples anywhere, so a single synthetic series"
echo "-- with value 1 is emitted (with the labels inferred from the selector: none here)."
promql_client -q "absent_over_time(nonexistent_metric[5m])"

echo "-- absent_over_time({__name__!=\"\",instance=\"host1\"}[3m]): host1 has both up and up2, and both have"
echo "-- samples in the range; series differing only by __name__ must not clash inside the presence grid."
promql_client -q 'absent_over_time({__name__!="",instance="host1"}[3m])'

echo "-- absent_over_time(up{instance=\"nohost\"}[5m]): no series matches, so the synthetic series"
echo "-- carries the equality-matcher label instance=\"nohost\"."
promql_client -q 'absent_over_time(up{instance="nohost"}[5m])'

echo "-- quantile_over_time(0.5, up[3m]): median of the in-range samples."
echo "-- host1 = 20 (median of 10,20,30), host2 = 15 (median of 5,15,25), host3 = 100."
promql_client -q "quantile_over_time(0.5, up[3m])" | sort

echo "-- quantile_over_time(0, up[3m]): minimum of the in-range samples."
promql_client -q "quantile_over_time(0, up[3m])" | sort

echo "-- quantile_over_time(1, up[3m]): maximum of the in-range samples."
promql_client -q "quantile_over_time(1, up[3m])" | sort

echo "-- predict_linear(up[3m], 0): the fitted value at the evaluation time."
echo "-- host1 -> 30, host2 -> 25, host3 -> 100."
promql_client -q "predict_linear(up[3m], 0)" | sort

echo "-- predict_linear(up[3m], 60): extrapolate 60s past the evaluation time."
echo "-- host1 -> 40 (30 + 60*1/6), host2 -> 35 (25 + 60*1/6), host3 -> 100 (flat)."
promql_client -q "predict_linear(up[3m], 60)" | sort

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
