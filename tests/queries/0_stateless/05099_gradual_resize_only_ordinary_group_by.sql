-- The gradual pre-aggregation resize is a property of the ordinary `GROUP BY` pre-aggregation only
-- (`AggregatingStep::enableGradualResize`). ClickHouse also builds `AggregatingStep` on its own for
-- internal aggregations; those must keep the strict resize, as the descriptions of both
-- `*_for_gradual_resize` settings promise. Two such carriers are checked here:
--  * the merge of the states read from an aggregate projection: when every part has the
--    projection, the planned `GROUP BY` step is turned into a merge-only step over the projection;
--  * the deduplication that `query_plan_optimize_lazy_final` plans for `FINAL` on a
--    `ReplacingMergeTree` table, an aggregation keyed by the sorting key that is built inside a
--    source processor and spliced into the query pipeline at run time.
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation resize
-- entirely, so the sources have to be `MergeTree` tables.

DROP TABLE IF EXISTS test_gradual_resize_projection;
DROP TABLE IF EXISTS test_gradual_resize_lazy_final;

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), which on a loaded CI runner collapses the pipeline to a
-- single stream and removes every resize processor. Pin it off, the assertions below are about
-- the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
-- The number of read streams is capped a second time by the minimum number of marks per
-- concurrent read, which is derived from `index_granularity_bytes` - a randomized `MergeTree`
-- setting. A small granularity in bytes makes that cap huge, collapses the read to a single stream
-- and removes every resize processor from the pipeline. Pin it off for the same reason.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;

-- 1. Aggregate projection.
CREATE TABLE test_gradual_resize_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, sum(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;
-- Distinct keys, so that the projection is as large as the table and is read in several streams.
INSERT INTO test_gradual_resize_projection SELECT number, number FROM numbers(200000);

-- Positive control: the same query over the table itself takes the gradual path.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v)
    FROM test_gradual_resize_projection
    GROUP BY k
    SETTINGS optimize_use_projections = 0
)
WHERE explain LIKE '%GradualResize%';

-- Reading the projection merges its states with the strict resize. `force_optimize_projection`
-- makes sure the projection is really used.
SELECT countIf(explain LIKE '%GradualResize%'), countIf(explain LIKE '%Resize%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(v)
    FROM test_gradual_resize_projection
    GROUP BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1
);

-- 2. Lazy `FINAL`. Its pipeline is not visible in `EXPLAIN PIPELINE`: the deduplicating
-- aggregation is built by the source at run time, hence the introspection through
-- `processors_profile_log`.
-- The optimization itself is planned only for the analyzer
-- (`QueryPlanOptimizationSettings::optimize_lazy_final` is conjoined with `allow_experimental_analyzer`),
-- so this half of the test asks for it explicitly instead of following the lane default.
SET enable_analyzer = 1;

CREATE TABLE test_gradual_resize_lazy_final (k UInt64, ver UInt64, v UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES test_gradual_resize_lazy_final;
INSERT INTO test_gradual_resize_lazy_final SELECT number, 1, number FROM numbers(100000);
INSERT INTO test_gradual_resize_lazy_final SELECT number, 2, number * 10 FROM numbers(0, 100000, 2);

SET log_processors_profiles = 1;
SET query_plan_optimize_lazy_final = 1;
SET min_filtered_ratio_for_lazy_final = 0;

SELECT count(), sum(v) FROM test_gradual_resize_lazy_final FINAL WHERE k % 4 = 0
    FORMAT Null SETTINGS log_comment = '05099_lazy_final';

-- Results are unaffected.
SELECT count(), sum(v) FROM test_gradual_resize_lazy_final FINAL WHERE k % 4 = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The `event_time` bound keeps the log scans cheap: without it every flaky-check rerun scans all
-- the log rows accumulated by the earlier runs.
SELECT
    countIf(name = 'LazyReadReplacingFinalSource') > 0 AS lazy_final_used,
    countIf(name = 'AggregatingTransform') > 1 AS has_parallel_aggregation,
    countIf(name = 'Resize') > 0 AS has_strict_resize,
    countIf(name = 'GradualResize') AS gradual_resizes
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '05099_lazy_final'
) AS q ON p.query_id = q.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE;

DROP TABLE test_gradual_resize_lazy_final;
DROP TABLE test_gradual_resize_projection;
