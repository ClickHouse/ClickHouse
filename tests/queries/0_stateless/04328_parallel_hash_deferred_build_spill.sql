-- A small `max_bytes_before_external_join` is crossed while the deferred build still stores the right blocks, before
-- any key is in a map. `SpillingHashJoin` hands the stored blocks to `GraceHashJoin`, which must lose or duplicate no
-- row. The streaming build without a threshold gives the reference.

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

CREATE TABLE t_build (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_build
SELECT k, concat('key_', toString(k)), k * 7
FROM
(
    SELECT number AS k, arrayJoin(range(multiIf(number % 16 = 0, 17, number % 4 = 0, 3, 1))) AS copy
    FROM numbers(8000)
);

INSERT INTO t_probe
SELECT k, concat('key_', toString(k)), k * 13
FROM (SELECT 4000 + intDiv(number, 2) AS k FROM numbers(16000));

SET enable_hash_join_deferred_build = 0;
SET max_bytes_before_external_join = 0;

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;

SET enable_hash_join_deferred_build = 1;
SET max_bytes_before_external_join = 131072;
SET log_comment = '04328_parallel_hash_deferred_build_spill';

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;

SYSTEM FLUSH LOGS query_log;
SELECT
    'switched to grace join',
    count() > 0,
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04328_parallel_hash_deferred_build_spill';

DROP TABLE t_build;
DROP TABLE t_probe;
