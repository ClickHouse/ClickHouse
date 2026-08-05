#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

-- Insert 3 series with values 30, 10, 20 (deliberately unsorted).
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'up', {'instance':'host2'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'up', {'instance':'host3'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 30),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 20);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- sort(up): ascending by value (10, 20, 30)"
promql_client -q "sort(up)"

echo "-- sort_desc(up): descending by value (30, 20, 10)"
promql_client -q "sort_desc(up)"

echo "-- sort(sort_desc(up)): ascending again (cancels out)"
promql_client -q "sort(sort_desc(up))"

echo "-- sort on expression: sort(up * 2) ascending (20, 40, 60)"
promql_client -q "sort(up * 2)"

echo "-- sort puts NaN after numeric values"
promql_client -q "sort(up * ((up - 10) / (up - 10)))"

echo "-- sort_desc puts NaN after numeric values"
promql_client -q "sort_desc(up * ((up - 10) / (up - 10)))"

echo "-- sort_desc is applied before unary negation"
promql_client -q "-sort_desc(up)"

echo "-- sort_desc order survives a non-monotonic value transformation"
promql_client -q "abs(sort_desc(up - 25))"

echo "-- sort_desc order follows the output side of vector matching"
promql_client -q "sort_desc(up) * on(instance) up"

echo "-- sort_desc order follows the right output side with group_right"
promql_client -q "up * on(instance) group_right sort_desc(up)"

echo "-- and preserves the order of its left argument"
promql_client -q "sort_desc(up) and up"

echo "-- unless preserves the order of its left argument"
promql_client -q 'sort_desc(up) unless up{instance="host2"}'

echo "-- or preserves an ordered left prefix"
promql_client -q "sort_desc(up) or vector(99)"

echo "-- or preserves an ordered right suffix"
promql_client -q "vector(99) or sort_desc(up)"

echo "-- or preserves the relative order within an unsorted side that has multiple rows"
# max_threads is pinned here because the row order within the unsorted side comes from physical
# join/block emission order (no ORDER BY, since neither `up{host2}` nor `up{host3}` carries a
# `sort_key`), which the test's random settings otherwise could reorder run to run.
promql_client -q 'sort_desc(up{instance="host1"}) or (up{instance="host2"} or up{instance="host3"})' --max_threads 1

echo "-- sort_desc ordering does not survive a subquery and range function"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
SELECT
    (SELECT groupArray(value) FROM prometheusQuery(ts, 'last_over_time(sort_desc(up)[1m:1m])', toDateTime64(1700000040, 3, 'UTC')))
    =
    (SELECT groupArray(value) FROM prometheusQuery(ts, 'last_over_time(up[1m:1m])', toDateTime64(1700000040, 3, 'UTC')))
SETTINGS max_threads = 1
"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
