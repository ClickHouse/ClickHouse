-- The two settings that shape the partition plan of the hash join.
-- `hash_join_max_fanout_per_pass` bounds how many partitions one scatter pass writes to.
-- `hash_join_cap_partitions_by_l1_descriptors` limits the plan to the descriptors that fit in a
-- quarter of L1. Every variant must return the same rows. The query log shows the partition count
-- each got. Neither setting changes the partition count at this size. The fan-out bound only splits
-- the scatter into passes. The descriptor cap allows far more partitions than 300000 keys get.

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

CREATE TABLE t_phj_scatter_build (k UInt64, ks String, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_phj_scatter_probe (k UInt64, ks String, p UInt64) ENGINE = MergeTree ORDER BY k;

-- 300000 distinct keys plan well above four partitions on every cache size CI runs on.
INSERT INTO t_phj_scatter_build SELECT number, toString(number), number * 3 FROM numbers(300000);
INSERT INTO t_phj_scatter_probe SELECT number % 400000, toString(number % 400000), number FROM numbers(600000);

SELECT 'default plan', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa)
SETTINGS log_comment = '05137 default';

SELECT 'two partitions per pass: several passes, same plan', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 2) AS pa)
SETTINGS log_comment = '05137 two per pass';

SELECT 'descriptor cap off', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', hash_join_cap_partitions_by_l1_descriptors = 0) AS pa)
SETTINGS log_comment = '05137 cap off';

-- Several scatter passes with non-joined rows, with a generic key type, and with duplicate keys arriving
-- in build blocks of 100000 rows.
SELECT 'two partitions per pass, right all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_phj_scatter_probe WHERE k < 200000) AS p RIGHT JOIN t_phj_scatter_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 2) AS pa);

SELECT 'two partitions per pass, string key', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 2) AS pa);

SELECT 'two partitions per pass, 8 rows per key from 100000-row build blocks', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_scatter_probe AS p INNER JOIN (SELECT number % 100000 AS k, number AS v FROM numbers(800000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 2, max_block_size = 100000) AS pa);

SELECT '-- values outside 2..32768 are rejected';
SELECT count() FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 65536; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_phj_scatter_probe AS p INNER JOIN t_phj_scatter_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'hash', hash_join_max_fanout_per_pass = 1; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT '-- all three plans partitioned, and into the same number of partitions';
SELECT count(), uniqExact(partitions), min(partitions) > 1
FROM
(
    SELECT log_comment, ProfileEvents['HashJoinPartitions'] AS partitions
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment LIKE '05137 %' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1 BY log_comment
);

DROP TABLE t_phj_scatter_build;
DROP TABLE t_phj_scatter_probe;
