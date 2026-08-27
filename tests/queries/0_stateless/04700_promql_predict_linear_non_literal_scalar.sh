#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `predict_linear(v range-vector, t scalar)` must accept a `t` that is a general
# PromQL scalar expression, not only a literal constant. `scalar(sum(vector(...)))` type-checks as
# a PromQL scalar but is only known at query-execution time (it is represented internally as
# StoreMethod::SINGLE_SCALAR, a single-row scalar subquery, rather than StoreMethod::CONST_SCALAR,
# a compile-time literal). See the same evaluation time / timestamp note as
# 04551_promql_phase3_range_functions.sh for why 1700000000 is used as the evaluation instant.

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

-- A single series, sampled every 60s and ending at the evaluation time 1700000000:
-- linear ramp 10 -> 20 -> 30 (slope +1/6 per second), same as host1 in 04551_promql_phase3_range_functions.sh.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999880, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1699999940, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- predict_linear(up[3m], 0) with a literal t: sanity check, fitted value at the evaluation time is 30."
promql_client -q "predict_linear(up[3m], 0)"

echo "-- predict_linear(up[3m], scalar(sum(vector(0)))) with a non-literal (SINGLE_SCALAR) t: same result, 30."
promql_client -q "predict_linear(up[3m], scalar(sum(vector(0))))"

echo "-- predict_linear(up[3m], 60) with a literal t: sanity check, extrapolated value is 40 (30 + 60*1/6)."
promql_client -q "predict_linear(up[3m], 60)"

echo "-- predict_linear(up[3m], scalar(sum(vector(60)))) with a non-literal (SINGLE_SCALAR) t: same result, 40."
promql_client -q "predict_linear(up[3m], scalar(sum(vector(60))))"

# A `t` derived from time() reaches this helper as a SINGLE_SCALAR whose value has already been
# materialized through the table's own value column type. On Float64 nothing rounds, so the horizon
# equals the evaluation instant exactly and must agree with the bare-time() spelling below
# (on a Float32 TimeSeries table this shape is rejected instead - see test_evaluation.py).
echo "-- predict_linear(up[3m], scalar(sum(vector(time())))): SINGLE_SCALAR t = evaluation instant, prediction 30 + 1700000000/6."
promql_client -q "predict_linear(up[3m], scalar(sum(vector(time()))))"

echo "-- predict_linear(up[3m], time()): bare time() t, same horizon, results must match."
promql_client -q "predict_linear(up[3m], time())"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
