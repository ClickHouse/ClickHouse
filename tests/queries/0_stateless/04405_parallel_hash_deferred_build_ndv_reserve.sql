-- The deferred build reserves for the distinct keys, not for the rows: 50000 keys, each 20 times. Under a 64 MiB
-- threshold the projection counts the 50000 keys, so the build fits and does not spill.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_build SELECT number % 50000 AS k, number AS v FROM numbers(1000000);
INSERT INTO t_probe SELECT number AS k, number * 7 AS v FROM numbers(60000);

SET enable_hash_join_deferred_build = 0;
SET max_bytes_before_external_join = 0;

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

SET enable_hash_join_deferred_build = 1;
SET max_bytes_before_external_join = 0;
SET log_comment = '04405_ndv_standalone';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

SET max_bytes_before_external_join = 67108864;
SET log_comment = '04405_ndv_capped_no_spill';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;

SELECT
    'reserve ~ ndv not rows',
    max(reserved) BETWEEN 40000 AND 150000
FROM
(
    SELECT ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] AS reserved
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment = '04405_ndv_standalone'
);

SELECT
    'capped build did not spill',
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04405_ndv_capped_no_spill';

DROP TABLE t_build;
DROP TABLE t_probe;
