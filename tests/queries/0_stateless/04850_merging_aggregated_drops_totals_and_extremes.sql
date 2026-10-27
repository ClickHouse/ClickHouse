-- Tags: distributed

DROP TABLE IF EXISTS t_04850;
DROP VIEW IF EXISTS v_04850;
DROP VIEW IF EXISTS v2_04850;
DROP TABLE IF EXISTS d_04850;

CREATE TABLE t_04850 (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04850 VALUES (1, 10), (2, 20);
CREATE VIEW v_04850 AS SELECT k, v FROM t_04850;
CREATE VIEW v2_04850 AS SELECT k, v FROM t_04850;
CREATE TABLE d_04850 (k UInt8, v UInt32)
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_04850');

-- The Merge spans three children reading the same two rows, so every value below is derivable by
-- hand: 3 * (10 + 20) = 90 overall, and per key 3 * 10 = 30 and 3 * 20 = 60.

-- Part 1: the shapes that used to abort while the pipeline was being built.
--
-- Two children must reach the merge step still carrying a totals port, so that Pipe::uniteTotals
-- builds its Concat -> Limit pair. inject_random_order_for_select_without_order_by is the only
-- producer of that state found here, and it is a development-only setting, so these arms assert
-- only that the pipeline can be built and deliberately assert no value. EXPLAIN PIPELINE is what
-- builds it, and building is where the defect is, so it reaches the defect; it also keeps these
-- arms clear of two unrelated pre-existing chunk-info errors that fire only once such a pipeline
-- actually runs.
--
-- Each arm also asserts which merge branch it reached, so a later plan change cannot silently move
-- an arm onto a branch it is not meant to cover.
SET enable_analyzer = 1;
SET inject_random_order_for_select_without_order_by = 1;

SELECT '-- memory-efficient branch, several merge threads';
-- Several merge threads resize the pipe and attach MergingAggregatedBucketTransform through
-- Pipe::addSimpleTransform, which applies it to totals_port too. That is the connect that used to
-- fail. The multiplicity is what tells the two sub-branches apart: one transform per merge thread
-- here, a single one below. The count is pinned in the same query, so 4 is exact, and asserting it
-- also confirms the resize honoured the setting.
SELECT count() > 0 AS reached_several_merge_threads
FROM (
    EXPLAIN PIPELINE
    SELECT sum(v) FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$') WITH TOTALS
    SETTINGS distributed_aggregation_memory_efficient = 1,
             aggregation_memory_efficient_merge_threads = 4, max_threads = 4
) WHERE explain ILIKE '%MergingAggregatedBucketTransform × 4%';

SELECT '-- memory-efficient branch, single merge thread';
-- A single merge thread returns early via addTransform, so the stale port instead survives into
-- TotalsHavingStep, which requires it to be null. One bare transform, so the multiplicity suffix
-- must be absent.
SELECT
    countIf(explain ILIKE '%MergingAggregatedBucketTransform%') > 0 AS reached_bucket_transform,
    countIf(explain ILIKE '%MergingAggregatedBucketTransform ×%') AS several_merge_threads
FROM (
    EXPLAIN PIPELINE
    SELECT sum(v) FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$') WITH TOTALS
    SETTINGS distributed_aggregation_memory_efficient = 1,
             aggregation_memory_efficient_merge_threads = 1, max_threads = 1
);

SELECT '-- non-memory-efficient branch';
-- This branch uses MergingAggregatedTransform and never the bucket transform.
SELECT
    countIf(explain ILIKE '%MergingAggregatedTransform%') > 0 AS reached_plain_merge,
    countIf(explain ILIKE '%MergingAggregatedBucketTransform%') AS bucket_transforms
FROM (
    EXPLAIN PIPELINE
    SELECT sum(v) FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$') WITH TOTALS
    SETTINGS distributed_aggregation_memory_efficient = 0, max_threads = 4
);

SET inject_random_order_for_select_without_order_by = 0;

-- Part 2: the values, with no development-only setting, so these arms assert real numbers. The
-- totals a merge step reports are re-derived from the merged states, so discarding the ones the
-- children computed must leave every result unchanged.

SELECT '-- ungrouped value then TOTALS row, both 90';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$') WITH TOTALS
SETTINGS distributed_aggregation_memory_efficient = 1,
         aggregation_memory_efficient_merge_threads = 1, max_threads = 1;

SELECT '-- same query without WITH TOTALS, still 90';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$')
SETTINGS distributed_aggregation_memory_efficient = 1,
         aggregation_memory_efficient_merge_threads = 1, max_threads = 1;

SELECT '-- grouped: 1/30, 2/60, then a 0/90 TOTALS row';
SELECT k, sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|v2_04850|d_04850)$')
GROUP BY k WITH TOTALS ORDER BY k
SETTINGS distributed_aggregation_memory_efficient = 1,
         aggregation_memory_efficient_merge_threads = 1, max_threads = 1;

SELECT '-- ground truth: the same three copies with no Merge and no Distributed';
SELECT k, sum(v) AS s FROM (
    SELECT * FROM t_04850 UNION ALL SELECT * FROM t_04850 UNION ALL SELECT * FROM t_04850
) GROUP BY k WITH TOTALS ORDER BY k;

DROP TABLE d_04850;
DROP VIEW v2_04850;
DROP VIEW v_04850;
DROP TABLE t_04850;
