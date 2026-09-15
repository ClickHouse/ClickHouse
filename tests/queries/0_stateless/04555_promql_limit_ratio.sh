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
DROP TABLE IF EXISTS ts_data;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;

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
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 3),
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 4),
    ('00000000-0000-0000-0000-000000000005', toDateTime64(1700000000, 3, 'UTC'), 5),
    ('00000000-0000-0000-0000-000000000006', toDateTime64(1700000000, 3, 'UTC'), 6);
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

echo "-- limit_ratio(1, up): the whole ratio, keeps all 6 series."
promql_count "limit_ratio(1, up)"

echo "-- limit_ratio(0, up): empty ratio, keeps nothing."
promql_count "limit_ratio(0, up)"

echo "-- limit_ratio(-1, up): the whole complement, keeps all 6 series."
promql_count "limit_ratio(-1, up)"

echo "-- limit_ratio(1.5, up): out-of-range r is clamped to 1, keeps all 6 series."
promql_count "limit_ratio(1.5, up)"

echo "-- limit_ratio(0.5, up) and limit_ratio(-0.5, up) keep complementary, reproducible subsets."
echo -n "r=0.5:  "; promql_instances "limit_ratio(0.5, up)"
echo -n "r=-0.5: "; promql_instances "limit_ratio(-0.5, up)"

echo "-- limit_ratio keeps the original series labels (including the metric name)."
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 \
    -q "limit_ratio(1, up)" | sort | head -1

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
