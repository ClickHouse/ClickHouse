-- The deferred build counts only the keys it will insert: a key that is NULL or fails the right-side ON condition is
-- not counted, so it does not enlarge the reserve. 200000 distinct right keys, of which `r.keep = 1` keeps 2000. A `LEFT`
-- join keeps the condition as an ON filter of the right side instead of pushing it into the scan.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build SELECT number AS k, (number < 2000) AS keep, number * 3 AS v FROM numbers(200000);
INSERT INTO t_probe SELECT number AS k, number * 7 AS v FROM numbers(200000);

SET enable_hash_join_deferred_build = 0;
SELECT 'mask', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1;

SET enable_hash_join_deferred_build = 1;
SET log_comment = '04410_on_mask';
SELECT 'mask', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_probe l LEFT JOIN t_build r ON l.k = r.k AND r.keep = 1;

SET log_comment = '';
SYSTEM FLUSH LOGS query_log;

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
