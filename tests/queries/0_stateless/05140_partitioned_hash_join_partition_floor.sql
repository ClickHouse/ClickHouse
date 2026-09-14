-- A build with few distinct keys but many rows: the table is small, so the cache rule alone would
-- pick one partition and one worker would insert every row after the barrier. From
-- `parallel_hash_join_threshold` rows on, `partitioned_hash` takes one partition per worker instead
-- (never more than distinct keys), as `parallel_hash` has one table per slot. Results must match `hash`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS t_pf_build;
DROP TABLE IF EXISTS t_pf_probe;
DROP TABLE IF EXISTS t_pf_build_str;
DROP TABLE IF EXISTS t_pf_probe_str;

-- UInt64 keys: a UInt16 key would take the direct-index map, which has no ranges to split.
CREATE TABLE t_pf_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(number % 1024) AS k, if(number % 13 = 0, NULL, toUInt64(number % 1024)) AS kn, number AS v FROM numbers(2000000);

CREATE TABLE t_pf_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(number % 1500) AS k, number AS p FROM numbers(2048);

SELECT '-- inner all';
SELECT count(), sum(cityHash64(p.p, b.k, b.v)) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, b.v)) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 inner all';

SELECT '-- right all on a nullable key';
SELECT count(), sum(cityHash64(p.p, ifNull(b.kn, 0), b.v)) FROM t_pf_probe AS p RIGHT JOIN t_pf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, ifNull(b.kn, 0), b.v)) FROM t_pf_probe AS p RIGHT JOIN t_pf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 right all nullable';

SELECT '-- full all';
SELECT count(), sum(cityHash64(p.p, b.k, b.v)) FROM t_pf_probe AS p FULL JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, b.v)) FROM t_pf_probe AS p FULL JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 full all';

SELECT '-- any left';
SELECT count(), sum(cityHash64(p.p, b.k)) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k)) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 any left';

SELECT '-- semi and anti left with the floor engaged';
SELECT count(), sum(cityHash64(p.p)) FROM t_pf_probe AS p SEMI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p)) FROM t_pf_probe AS p SEMI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 semi left';
SELECT count(), sum(cityHash64(p.p)) FROM t_pf_probe AS p ANTI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p)) FROM t_pf_probe AS p ANTI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 anti left';

SELECT '-- any left keeping the last row per key';
SELECT count(), sum(cityHash64(p.p, b.k)) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1;
SELECT count(), sum(cityHash64(p.p, b.k)) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', join_any_take_last_row = 1, log_comment = '05140 any left last row';

SELECT '-- string key over the same few keys';
CREATE TABLE t_pf_build_str ENGINE = MergeTree ORDER BY tuple() AS
SELECT toString(number % 1024) AS ks, number AS v FROM numbers(2000000);
CREATE TABLE t_pf_probe_str ENGINE = MergeTree ORDER BY tuple() AS
SELECT toString(number % 1500) AS ks, number AS p FROM numbers(2048);
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p INNER JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p INNER JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 string inner all';
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p RIGHT JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p RIGHT JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 string right all';
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p FULL JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.ks, b.v)) FROM t_pf_probe_str AS p FULL JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05140 string full all';

SELECT '-- below the threshold the small table keeps one partition';
SELECT count(), sum(cityHash64(p.p, b.k, b.v)) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k
SETTINGS join_algorithm = 'partitioned_hash', parallel_hash_join_threshold = 100000000, log_comment = '05140 below threshold';

SYSTEM FLUSH LOGS query_log;

SELECT '-- partitions above the threshold: 4 (one per thread); below: 1';
SELECT log_comment, ProfileEvents['PartitionedHashJoinPartitions'], ProfileEvents['PartitionedHashJoinLeafRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05140 %'
ORDER BY log_comment;

DROP TABLE t_pf_build;
DROP TABLE t_pf_probe;
DROP TABLE t_pf_build_str;
DROP TABLE t_pf_probe_str;
