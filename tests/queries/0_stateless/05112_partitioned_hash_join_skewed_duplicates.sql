-- Duplicate-heavy and skewed builds under `partitioned_hash` against `hash`. One key has more rows
-- than one run of duplicates can hold, so its rows span several runs. A Zipf-like skew puts most
-- rows on a few keys. Duplicates of a key spread over many small build blocks. Several thread
-- counts are used. Each line prints the row count and whether the two results are equal.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_ird_build;
DROP TABLE IF EXISTS t_ird_probe;
DROP TABLE IF EXISTS t_ird_zipf;

-- 400000 rows: key 7 takes every tenth row (40000 of them, more than the 32766 one run of duplicates
-- holds); the other 60000 keys have about six rows each.
CREATE TABLE t_ird_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT if(number % 10 = 0, 7, number % 60000) AS k, number AS v FROM numbers(400000);

CREATE TABLE t_ird_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, number AS p FROM numbers(70000);

-- A heavily skewed build: 300000 rows over 540 keys (the cube of a uniform variate, scaled). The
-- count runs from about 185 rows per key at the tail to about 37000 at key 0. Every key's duplicates
-- fill one long run or several.
CREATE TABLE t_ird_zipf ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(floor(pow((number % 1000000) / 1000000., 3) * 20000)) AS k, number AS v FROM numbers(300000);

SELECT 'inner all, saturated key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05112 saturated';

SELECT 'right all, saturated key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'right any, saturated key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ird_probe AS p ANY RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ird_probe AS p ANY RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'inner any, saturated key', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ird_probe AS p ANY INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ird_probe AS p ANY INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

-- Small build blocks: a key's rows arrive in many different blocks and, when the scatter is split into
-- block ranges, in different ranges.
SELECT 'inner all, duplicates across 777-row blocks', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_block_size = 777) AS pa);

SELECT 'full all, duplicates across 777-row blocks, max_threads 1', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p FULL JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p FULL JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_block_size = 777, max_threads = 1) AS pa);

SELECT 'left all, max_threads 16', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p LEFT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p LEFT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS pa);

SELECT 'inner all, zipf', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05112 zipf';

SELECT 'right all, zipf', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

-- A skewed probe over the skewed build: every thousandth build row probes back, so the hot keys are
-- probed many times and each hit emits thousands of rows.
SELECT 'inner all, zipf build probed by a zipf sample', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.v, b.v))) FROM (SELECT * FROM t_ird_zipf WHERE v % 1000 = 0) AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.v, b.v))) FROM (SELECT * FROM t_ird_zipf WHERE v % 1000 = 0) AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SYSTEM FLUSH LOGS query_log;

SELECT
    'saturated build: every row inserted, duplicate storage used',
    ProfileEvents['HashJoinInsertedRows'] = (SELECT count() FROM t_ird_build),
    ProfileEvents['HashJoinDuplicateRunBytes'] >= 8 * (SELECT count() FROM t_ird_build WHERE k = 7)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112 saturated' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT
    'zipf build: every row inserted, duplicate storage used',
    ProfileEvents['HashJoinInsertedRows'] = (SELECT count() FROM t_ird_zipf),
    ProfileEvents['HashJoinDuplicateRunBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112 zipf' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_ird_build;
DROP TABLE t_ird_probe;
DROP TABLE t_ird_zipf;
