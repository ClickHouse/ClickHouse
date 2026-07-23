-- Tags: no-random-merge-tree-settings, no-old-analyzer
-- no-old-analyzer: lazy materialization (and thus __topKFilter) is gated on the
-- analyzer, so the EXPLAIN actions=1 assertion below never matches under the old one.

DROP TABLE IF EXISTS t_lazy_topk_pos;

CREATE TABLE t_lazy_topk_pos (k UInt32, path String, file_name String, file_size UInt64)
ENGINE = MergeTree ORDER BY k;

-- Two parts; cityHash64 keeps file_size deterministic across runs.
INSERT INTO t_lazy_topk_pos SELECT 1, concat('/some/long/path/prefix/file_', toString(number)), concat('file_', toString(number)), cityHash64(number) % 1000000000 FROM numbers(20000);
INSERT INTO t_lazy_topk_pos SELECT 1, concat('/some/long/path/prefix/file_', toString(number)), concat('file_', toString(number)), cityHash64(number + 1000000) % 1000000000 FROM numbers(20000);

-- LIMIT 20 must stay below the max_limit_for_* thresholds.
SET max_threads = 1;
SET max_block_size = 8192;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 100;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET query_plan_optimize_lazy_materialization = 1;

-- Prove the plan actually reaches the buggy code: the top-k dynamic filter
-- (__topKFilter) must be installed AND the WHERE-only String column `path` must
-- be lazily materialized.
SELECT
    (countIf(explain LIKE '%__topKFilter(file_size)%') > 0)
AND (countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0)
FROM (
    EXPLAIN actions = 1
    SELECT path, file_name, file_size FROM t_lazy_topk_pos
    WHERE k = 1 AND path != '' ORDER BY file_size DESC LIMIT 20);

-- The lazy-materialized plan places the WHERE-only String column `path` before
-- the sort column `file_size`, so the top-k threshold must be read by name, not
-- from physical position 0. max_threads=1 forces the MergeSortingTransform path
-- that publishes the threshold. The optimized result must match the unoptimized
-- one; the only difference between the two queries is
-- query_plan_optimize_lazy_materialization.
SELECT
    (SELECT sum(cityHash64(path, file_name, file_size)) FROM (
        SELECT path, file_name, file_size FROM t_lazy_topk_pos
        WHERE k = 1 AND path != '' ORDER BY file_size DESC LIMIT 20))
  = (SELECT sum(cityHash64(path, file_name, file_size)) FROM (
        SELECT path, file_name, file_size FROM t_lazy_topk_pos
        WHERE k = 1 AND path != '' ORDER BY file_size DESC LIMIT 20
        SETTINGS query_plan_optimize_lazy_materialization = 0));

DROP TABLE t_lazy_topk_pos;
