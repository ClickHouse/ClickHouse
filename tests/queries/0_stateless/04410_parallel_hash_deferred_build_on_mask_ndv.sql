-- Tags: no-random-settings, no-random-merge-tree-settings

-- Follow-up to #108129: the deferred (exact-size) parallel_hash build estimates the distinct-key count
-- with a HyperLogLog over the scatter hashes. It must count only the keys that are actually inserted
-- into the build map. The replay (like the streaming build) skips a right-side row whose key is NULL or
-- that fails the right-side ON condition, so feeding the HLL the hashes of *every* source row would
-- over-size the map (and the spill projection) for those rejected keys -- a build the streaming path
-- keeps small could then spuriously spill or hit MEMORY_LIMIT_EXCEEDED.
--
-- Here the build (right) side has 200000 distinct keys, but a right-side ON condition (`r.keep = 1`)
-- keeps only ~2000 of them. A LEFT JOIN is used so the condition stays a build-side ON filter and is
-- not pushed down into the table scan. The deferred reserve must be ~2000 (the inserted keys), not
-- ~200000 (all keys). `query_plan_join_swap_table = 'false'` keeps the big table as the build side.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of the build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 200000 distinct build keys; only k < 2000 have keep = 1 (so ~2000 keys are insertable).
INSERT INTO t_build SELECT number AS k, (number < 2000) AS keep, number * 3 AS v FROM numbers(200000);
INSERT INTO t_probe SELECT number AS k, number * 7 AS v FROM numbers(200000);

-- Reference: plain hash join.
SET join_algorithm = 'hash';
SELECT 'mask', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1;

SET join_algorithm = 'parallel_hash';
SET log_comment = '04410_on_mask';
SELECT 'mask', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1;

SET log_comment = ''; -- so the assertion SELECTs below are not themselves matched by the filter
SYSTEM FLUSH LOGS query_log;

-- The deferred reserve is sized by the inserted (keep = 1) keys (~2000), not by all 200000 build keys.
-- A value near 200000 here means the HLL counted rows the replay never inserts.
SELECT
    'reserve sizes inserted keys not all keys',
    max(reserved) BETWEEN 1 AND 60000,
    max(reserved) > 0
FROM
(
    SELECT ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] AS reserved
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment = '04410_on_mask'
);

DROP TABLE t_build;
DROP TABLE t_probe;
