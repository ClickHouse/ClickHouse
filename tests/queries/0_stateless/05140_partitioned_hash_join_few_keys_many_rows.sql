-- A build with few distinct keys and many rows. The hash table is small, so sizing the partitions
-- by cache alone gives one partition, and one thread would insert every row. From
-- `parallel_hash_join_threshold` build rows on, `partitioned_hash` uses one partition per thread
-- instead (never more partitions than distinct keys), as `parallel_hash` has one table per thread.
-- Results must match `hash`.

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

-- `UInt64` keys: a `UInt16` key would use the direct-index table, which is never partitioned.
CREATE TABLE t_pf_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(number % 1024) AS k, if(number % 13 = 0, NULL, toUInt64(number % 1024)) AS kn, number AS v FROM numbers(2000000);

CREATE TABLE t_pf_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(number % 1500) AS k, number AS p FROM numbers(2048);

SELECT 'inner all', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 inner all';

SELECT 'right all on a nullable key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, ifNull(b.kn, 0), b.v))) FROM t_pf_probe AS p RIGHT JOIN t_pf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, ifNull(b.kn, 0), b.v))) FROM t_pf_probe AS p RIGHT JOIN t_pf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 right all nullable';

SELECT 'full all', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p FULL JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p FULL JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 full all';

SELECT 'any left', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 any left';

SELECT '-- semi and anti left';
SELECT 'semi left', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_pf_probe AS p SEMI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_pf_probe AS p SEMI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 semi left';

SELECT 'anti left', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_pf_probe AS p ANTI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_pf_probe AS p ANTI LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 anti left';

SELECT 'any left keeping the last row per key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_pf_probe AS p ANY LEFT JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', join_any_take_last_row = 1) AS pa)
SETTINGS log_comment = '05140 any left last row';

SELECT '-- string key over the same few keys';
CREATE TABLE t_pf_build_str ENGINE = MergeTree ORDER BY tuple() AS
SELECT toString(number % 1024) AS ks, number AS v FROM numbers(2000000);
CREATE TABLE t_pf_probe_str ENGINE = MergeTree ORDER BY tuple() AS
SELECT toString(number % 1500) AS ks, number AS p FROM numbers(2048);

SELECT 'string inner all', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p INNER JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p INNER JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 string inner all';

SELECT 'string right all', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p RIGHT JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p RIGHT JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 string right all';

SELECT 'string full all', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p FULL JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.ks, b.v))) FROM t_pf_probe_str AS p FULL JOIN t_pf_build_str AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05140 string full all';

SELECT 'below the threshold the small table keeps one partition', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k, b.v))) FROM t_pf_probe AS p INNER JOIN t_pf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', parallel_hash_join_threshold = 100000000) AS pa)
SETTINGS log_comment = '05140 below threshold';

SYSTEM FLUSH LOGS query_log;

SELECT '-- partitions above the threshold: 4 (one per thread); below: 1';
SELECT log_comment, ProfileEvents['HashJoinPartitions'], ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05140 %'
ORDER BY log_comment;

DROP TABLE t_pf_build;
DROP TABLE t_pf_probe;
DROP TABLE t_pf_build_str;
DROP TABLE t_pf_probe_str;
