-- Duplicate keys on the partitioned hash join. Coverage includes join kinds and strictness over
-- builds where every key has the same number of rows (1, 2, 3, 8, 300). Duplicates arrive in build
-- blocks of 100000 rows. One key has 32766 rows (its duplicates fill one run). One key has 32767
-- rows (its duplicates split into two runs). Also a zero key, string keys, and query log checks that
-- the duplicate storage used exactly 8 bytes per duplicate row, plus a header for the second run.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS t_sc_b;
DROP TABLE IF EXISTS t_sc_p;
DROP TABLE IF EXISTS t_sc_z;

-- 20000 keys x 8 rows, plus a zero key with 8 rows.
CREATE TABLE t_sc_b ENGINE = MergeTree ORDER BY tuple() AS
SELECT if(number < 8, 0, 1 + (number % 20000)) AS k, toString(if(number < 8, 0, 1 + (number % 20000))) AS ks, number AS v
FROM numbers(8 + 20000 * 8);

CREATE TABLE t_sc_p ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, toString(number) AS ks, number AS p FROM numbers(22000);

CREATE TABLE t_sc_z ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, number AS p FROM numbers(100);

SELECT 'dups 8, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa)
SETTINGS log_comment = '05138 dups 8';

SELECT 'dups 8, left all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, right all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p RIGHT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, full all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p FULL JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, inner any (key checksum)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.k))) FROM t_sc_p AS p ANY INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, left semi', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p SEMI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, left anti', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p ANTI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8, string keys', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'zero key, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 1, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number + 1 AS k, number AS v FROM numbers(20000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 2, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(40000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 3, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(60000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 300, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 1000 AS k, number AS v FROM numbers(300000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'dups 8 from 100000-row build blocks, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(160000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_block_size = 100000) AS pa);

-- One key of 32766 rows: its duplicates fill one run exactly, `8 * 32766` bytes. One key of 32767 rows:
-- its duplicates fill one run of 32766 and a second run with a 16-byte header, so `8 * 32767 + 16` bytes.
SELECT '32766-row key, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN (SELECT 7 AS k, number AS v FROM numbers(32766)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa)
SETTINGS log_comment = '05138 32766 rows';

SELECT '32767-row key, inner all', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN (SELECT 7 AS k, number AS v FROM numbers(32767)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa)
SETTINGS log_comment = '05138 32767 rows';

SYSTEM FLUSH LOGS query_log;

SELECT 'dups 8: duplicate storage bytes',
    ProfileEvents['HashJoinDuplicateRunBytes'] = 8 * (SELECT count() FROM t_sc_b)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05138 dups 8'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '32766-row key: duplicate storage bytes',
    ProfileEvents['HashJoinDuplicateRunBytes'] = 8 * 32766
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05138 32766 rows'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '32767-row key: duplicate storage bytes',
    ProfileEvents['HashJoinDuplicateRunBytes'] >= 8 * 32767
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05138 32767 rows'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_sc_b;
DROP TABLE t_sc_p;
DROP TABLE t_sc_z;
