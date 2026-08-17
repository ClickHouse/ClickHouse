#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# These queries read 3 rows, so the runtime is almost entirely per-`clickhouse-client` spawn cost.
# Every case that needs no shell therefore runs in one batch, with `SELECT` emitting the labels.
EVAL_TIME="toDateTime64(1700000000, 3, 'UTC')"

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

-- A second metric with its own name and an extra tag, one series per instance, in ascending
-- instance order - deliberately a different natural order than sort_desc(up) produces.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000011', 'mem', {'instance':'host1','job':'j1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000012', 'mem', {'instance':'host2','job':'j1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000013', 'mem', {'instance':'host3','job':'j1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    ('00000000-0000-0000-0000-000000000011', toDateTime64(1700000000, 3, 'UTC'), 1),
    ('00000000-0000-0000-0000-000000000012', toDateTime64(1700000000, 3, 'UTC'), 2),
    ('00000000-0000-0000-0000-000000000013', toDateTime64(1700000000, 3, 'UTC'), 3);

SELECT '-- sort(up): ascending by value (10, 20, 30)';
SELECT * FROM prometheusQuery(ts, 'sort(up)', $EVAL_TIME);

SELECT '-- sort_desc(up): descending by value (30, 20, 10)';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up)', $EVAL_TIME);

SELECT '-- sort(sort_desc(up)): ascending again (cancels out)';
SELECT * FROM prometheusQuery(ts, 'sort(sort_desc(up))', $EVAL_TIME);

SELECT '-- sort on expression: sort(up * 2) ascending (20, 40, 60)';
SELECT * FROM prometheusQuery(ts, 'sort(up * 2)', $EVAL_TIME);

SELECT '-- sort puts NaN after numeric values';
SELECT * FROM prometheusQuery(ts, 'sort(up * ((up - 10) / (up - 10)))', $EVAL_TIME);

SELECT '-- sort_desc puts NaN after numeric values';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up * ((up - 10) / (up - 10)))', $EVAL_TIME);

SELECT '-- sort_desc is applied before unary negation';
SELECT * FROM prometheusQuery(ts, '-sort_desc(up)', $EVAL_TIME);

SELECT '-- sort_desc order survives a non-monotonic value transformation';
SELECT * FROM prometheusQuery(ts, 'abs(sort_desc(up - 25))', $EVAL_TIME);

SELECT '-- sort_desc order follows the output side of vector matching';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up) * on(instance) up', $EVAL_TIME);

SELECT '-- sort_desc order follows the right output side with group_right';
SELECT * FROM prometheusQuery(ts, 'up * on(instance) group_right sort_desc(up)', $EVAL_TIME);

SELECT '-- and preserves the order of its left argument';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up) and up', $EVAL_TIME);

SELECT '-- unless preserves the order of its left argument';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up) unless up{instance=\"host2\"}', $EVAL_TIME);

SELECT '-- or preserves an ordered left prefix';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up) or vector(99)', $EVAL_TIME);

SELECT '-- or preserves an ordered right suffix';
SELECT * FROM prometheusQuery(ts, 'vector(99) or sort_desc(up)', $EVAL_TIME);

SELECT '-- the value order of topk (descending) is propagated as a sort order, so or preserves it as an ordered left prefix';
SELECT * FROM prometheusQuery(ts, 'topk(2, up) or vector(99)', $EVAL_TIME);

SELECT '-- topk with by(...) grouping carries a bucket-aware sort order (buckets consecutive in an unspecified hash-based order, values ordered within each bucket), so it composes with or as an ordered left prefix (all 3 singleton buckets pass through)';
-- ORDER BY normalizes away the hash-based bucket order; 04627_promql_topk_bottomk_grouped_order covers it.
SELECT * FROM prometheusQuery(ts, 'topk(2, up) by (instance) or vector(99)', $EVAL_TIME) ORDER BY tags;

SELECT '-- group_right takes both the labels and the order from the right (many) side, even for a comparison without bool whose surviving samples come from the left';
-- All 3 comparisons are true, so this isolates ordering from filtering: the values are up's,
-- the tags are mem's, and the order is sort_desc(mem)'s (host3, host2, host1), not sort(up)'s.
SELECT * FROM prometheusQuery(ts, 'sort(up) > on(instance) group_right sort_desc(mem)', $EVAL_TIME);

SELECT '-- or appends the unmatched right rows after the left rows, keeping each side''s own order';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up{instance=\"host1\"}) or (up{instance=\"host2\"} or up{instance=\"host3\"})', $EVAL_TIME);

SELECT '-- swapping the nested or operands flips the suffix: its order follows the or structure, not tag hashes';
SELECT * FROM prometheusQuery(ts, 'sort_desc(up{instance=\"host1\"}) or (up{instance=\"host3\"} or up{instance=\"host2\"})', $EVAL_TIME);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- or breaks ties within a side carrying no order of its own using a content hash of each row's tags"
# The suffix has no order of its own, so the sampling-key tiebreak decides it: the reference pins
# the sorted set and two runs must match. `unless` avoids `=~`, unusable in the dialect (#115071).
query='sort_desc(up{instance="host1"}) or (up unless up{instance="host1"})'
default_output=$(promql_client -q "$query")
echo "$default_output" | LC_ALL=C sort
diff <(echo "$default_output") <(promql_client -q "$query" --max_threads 1 --query_plan_join_swap_table false) && echo "OK: same row order regardless of max_threads/query_plan_join_swap_table"

echo "-- sort_desc ordering does not survive a subquery and range function"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
SELECT
    (SELECT groupArray(value) FROM prometheusQuery(ts, 'last_over_time(sort_desc(up)[1m:1m])', toDateTime64(1700000040, 3, 'UTC')))
    =
    (SELECT groupArray(value) FROM prometheusQuery(ts, 'last_over_time(up[1m:1m])', toDateTime64(1700000040, 3, 'UTC')))
SETTINGS max_threads = 1;

DROP TABLE ts;
DROP TABLE ts_data;
DROP TABLE ts_tags;
DROP TABLE ts_metrics;
"
