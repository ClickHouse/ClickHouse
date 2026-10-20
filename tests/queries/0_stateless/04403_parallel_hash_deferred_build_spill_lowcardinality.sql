-- The same hand-over with a `LowCardinality(String)` key. 2000 right rows share one long value: the dictionary stays
-- small, while the map would copy the value for each distinct key.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;
SET enable_parallel_replicas = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_build
SELECT if(number < 2000, repeat('hot_', 50), concat('k_', toString(number % 1500))) AS k, number AS v
FROM numbers(8000);

INSERT INTO t_probe
SELECT concat('k_', toString(number % 1500)) AS k, number AS v
FROM numbers(8000);

SET enable_hash_join_deferred_build = 0;
SET max_bytes_before_external_join = 0;

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

SET enable_hash_join_deferred_build = 1;
SET max_bytes_before_external_join = 131072;
SET log_comment = '04403_parallel_hash_deferred_build_spill_lowcardinality';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;
SELECT
    'switched to grace join',
    count() > 0,
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04403_parallel_hash_deferred_build_spill_lowcardinality';

DROP TABLE t_build;
DROP TABLE t_probe;
