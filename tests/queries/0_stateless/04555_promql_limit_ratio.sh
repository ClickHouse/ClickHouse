#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# limit_ratio(r, v) keeps a deterministic pseudo-random ratio r of the input series (r in [-1, 1]).
# The selection is based on a stable per-series hash, so r and -r keep complementary subsets and
# the choice is reproducible. Six series are used; each has a single sample at the evaluation time.

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
DROP TABLE IF EXISTS ts;

CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 1)]),
    ('up', {'instance':'host2'}, [(toDateTime64(1700000000, 3, 'UTC'), 2)]),
    ('up', {'instance':'host3'}, [(toDateTime64(1700000000, 3, 'UTC'), 3)]),
    ('up', {'instance':'host4'}, [(toDateTime64(1700000000, 3, 'UTC'), 4)]),
    ('up', {'instance':'host5'}, [(toDateTime64(1700000000, 3, 'UTC'), 5)]),
    ('up', {'instance':'host6'}, [(toDateTime64(1700000000, 3, 'UTC'), 6)]);
"

promql_instances()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
        -q "$1" --format Values | grep -oE "'host[0-9]'" | tr -d "'" | sort | tr '\n' ' '
    echo
}

promql_count()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
        -q "$1" | wc -l | tr -d ' '
}

promql_nan_ratio_error()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
        -q "$1" 2>&1 | grep -o "Ratio of aggregation operator limit_ratio must not be NaN" | head -1
}

echo "-- limit_ratio(1, up): the whole ratio, keeps all 6 series."
promql_count "limit_ratio(1, up)"

echo "-- limit_ratio(0, up): empty ratio, keeps nothing."
promql_count "limit_ratio(0, up)"

echo "-- limit_ratio(-1, up): the whole complement, keeps all 6 series."
promql_count "limit_ratio(-1, up)"

echo "-- limit_ratio(1.5, up): out-of-range r is clamped to 1, keeps all 6 series."
promql_count "limit_ratio(1.5, up)"

echo "-- limit_ratio(NaN, up): a NaN ratio is rejected, as in Prometheus."
promql_nan_ratio_error "limit_ratio(NaN, up)"

echo "-- limit_ratio(scalar(up), up): a computed NaN ratio (scalar() of 6 series) is rejected too."
promql_nan_ratio_error "limit_ratio(scalar(up), up)"

echo "-- The same for a computed ratio which varies along the grid of a subquery."
promql_nan_ratio_error "limit_ratio(scalar(up), up)[200s:100s]"

echo "-- A NaN ratio is rejected even if the vector is known to be empty when the query is built."
promql_nan_ratio_error "limit_ratio(NaN, clamp(up, 1, -1))"
promql_nan_ratio_error "limit_ratio(scalar(clamp(up, 1, -1)), clamp(up, 1, -1))"
promql_nan_ratio_error "limit_ratio(scalar(up), clamp(up, 1, -1))"

echo "-- limit_ratio(1, clamp(up, 1, -1)): a valid ratio over such a vector gives an empty result."
promql_count "limit_ratio(1, clamp(up, 1, -1))"

echo "-- Such an empty result keeps the evaluation time grid, in an instant and in a range query."
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
    -q "limit_ratio(1, clamp(up, 1, -1)) or vector(7)"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 \
    -q "SELECT * FROM prometheusQueryRange(ts, 'limit_ratio(1, clamp(up, 1, -1)) or vector(7)', 1699999940, 1700000000, 30)"

echo "-- limit_ratio(0.5, up) and limit_ratio(-0.5, up) keep complementary subsets, the same ones as Prometheus."
echo -n "r=0.5:  "; promql_instances "limit_ratio(0.5, up)"
echo -n "r=-0.5: "; promql_instances "limit_ratio(-0.5, up)"
echo -n "r=0.2:  "; promql_instances "limit_ratio(0.2, up)"
echo -n "r=-0.3: "; promql_instances "limit_ratio(-0.3, up)"

echo "-- limit_ratio keeps the original series labels (including the metric name)."
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
    -q "limit_ratio(1, up)" | sort | head -1

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
