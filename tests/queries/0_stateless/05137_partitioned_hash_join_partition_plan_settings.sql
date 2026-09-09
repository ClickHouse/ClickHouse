-- The two settings that shape the partition plan of `partitioned_hash`:
-- `partitioned_hash_join_max_fanout_per_pass` bounds how many partitions one scatter pass writes to,
-- and `partitioned_hash_join_cap_partitions_by_l1_descriptors` limits the plan to the descriptors
-- that fit in a quarter of L1. Every variant must return what `hash` returns, and the query log
-- shows the partition count each got. Neither setting changes the plan at this size: the ceiling
-- only splits the scatter into passes, and the cap binds far above the leaves 300000 keys need.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
-- The ProfileEvents below come from this server's query_log, so the builds must run here.
SET enable_parallel_replicas = 0;
-- A distinct-key count cached by an earlier run would feed the plan; every query must plan from
-- its own build.
SET collect_hash_table_stats_during_joins = 0;

DROP TABLE IF EXISTS t_phj_scatter_build;
DROP TABLE IF EXISTS t_phj_scatter_probe;

CREATE TABLE t_phj_scatter_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_phj_scatter_probe (k UInt64, p UInt64) ENGINE = MergeTree ORDER BY k;

-- 300000 distinct keys plan well above four partitions on every cache size CI runs on.
INSERT INTO t_phj_scatter_build SELECT number, number * 3 FROM numbers(300000);
INSERT INTO t_phj_scatter_probe SELECT number % 400000, number FROM numbers(600000);

SELECT '-- hash, for reference';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'hash';

SELECT '-- default plan';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'phj_scatter_default';

SELECT '-- two partitions per pass: several passes, same plan';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', partitioned_hash_join_max_fanout_per_pass = 2, log_comment = 'phj_scatter_narrow';

SELECT '-- descriptor cap off';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', partitioned_hash_join_cap_partitions_by_l1_descriptors = 0, log_comment = 'phj_scatter_uncapped';

SELECT '-- the ceiling must keep the first scatter pass within 15 bits';
SELECT count() FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', partitioned_hash_join_max_fanout_per_pass = 65536; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', partitioned_hash_join_max_fanout_per_pass = 1; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT '-- all three plans partitioned, and into the same number of partitions';
SELECT count(), uniqExact(partitions), min(partitions) > 1
FROM
(
    SELECT log_comment, ProfileEvents['PartitionedHashJoinPartitions'] AS partitions
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment LIKE 'phj_scatter_%' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1 BY log_comment
);

DROP TABLE t_phj_scatter_build;
DROP TABLE t_phj_scatter_probe;
