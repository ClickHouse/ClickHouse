-- The granules of a `Memory` table skipped by the query condition cache entry of the filter of the query are
-- not evaluated by `PREWHERE`, so they must not be recorded as having no rows satisfying `PREWHERE`.

SET use_query_condition_cache = 1, optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_memory_qcc_prewhere_after_where;
CREATE TABLE t_memory_qcc_prewhere_after_where (a UInt8, b UInt8) ENGINE = Memory;
-- The first granule has rows with `a = 1` but none with `b = 1`, the second has rows with both.
INSERT INTO t_memory_qcc_prewhere_after_where SELECT number % 2, number >= 8192 FROM numbers(16384) SETTINGS max_block_size = 16384, max_insert_threads = 1;

-- The filter of the query is evaluated by a single `FilterTransform`, which records the granules without
-- rows satisfying `a = 1 AND b = 1`. The next query has the same filter pushed down to the read, made of
-- `PREWHERE` and `WHERE`, and skips the first granule before evaluating `PREWHERE`.
SELECT count() FROM t_memory_qcc_prewhere_after_where WHERE a = 1 AND b = 1 SETTINGS query_plan_split_filter = 0, query_plan_merge_filters = 0;
SELECT count() FROM t_memory_qcc_prewhere_after_where PREWHERE a = 1 WHERE b = 1;
SELECT count() FROM t_memory_qcc_prewhere_after_where PREWHERE a = 1;
SELECT count() FROM t_memory_qcc_prewhere_after_where PREWHERE a = 1 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_memory_qcc_prewhere_after_where;
