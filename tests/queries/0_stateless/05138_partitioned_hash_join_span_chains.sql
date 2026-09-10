-- Exact-span duplicates under `join_algorithm = 'partitioned_hash'` against `hash`: kinds and
-- strictness over several multiplicities, a key of 32767 rows (one split), a zero key, string keys,
-- duplicates spread across small blocks (chains under a grouped budget), and one ProfileEvents check
-- that arena bytes equal `8 * dup_rows + 16 * headers`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_threads = 8;
SET max_block_size = 65536;
SET max_insert_block_size = 65536;

DROP TABLE IF EXISTS t_sc_b;
DROP TABLE IF EXISTS t_sc_p;
DROP TABLE IF EXISTS t_sc_sat;
DROP TABLE IF EXISTS t_sc_z;
DROP TABLE IF EXISTS t_sc_g;

-- 20000 keys × 8, plus a zero key with 8 rows.
CREATE TABLE t_sc_b ENGINE = MergeTree ORDER BY tuple() AS
SELECT if(number < 8, 0, 1 + (number % 20000)) AS k, toString(if(number < 8, 0, 1 + (number % 20000))) AS ks, number AS v
FROM numbers(8 + 20000 * 8);

CREATE TABLE t_sc_p ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, toString(number) AS ks, number AS p FROM numbers(22000);

-- One key, 32767 rows: one split, one header, `8 * 32767 + 16` arena bytes.
CREATE TABLE t_sc_sat ENGINE = MergeTree ORDER BY tuple() AS
SELECT 7 AS k, number AS v FROM numbers(32767);

CREATE TABLE t_sc_z ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, number AS p FROM numbers(100);

-- Duplicate-major 200000 keys × 8 (the `GroupedScatterExactSpans` gtest layout).
CREATE TABLE t_sc_g ENGINE = MergeTree ORDER BY tuple() AS
SELECT number % 200000 AS k, number AS v FROM numbers(1600000);

SELECT 'dups 8, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, left all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, right all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p RIGHT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p RIGHT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, full all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p FULL JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p FULL JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, inner any (key checksum)', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.k))) FROM t_sc_p AS p ANY INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.k))) FROM t_sc_p AS p ANY INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, left semi', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p SEMI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p SEMI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, left anti', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p ANTI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sc_p AS p ANTI LEFT JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 8, string keys', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'zero key, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 1, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number + 1 AS k, number AS v FROM numbers(20000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number + 1 AS k, number AS v FROM numbers(20000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 2, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(40000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(40000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 3, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(60000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 20000 AS k, number AS v FROM numbers(60000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 300, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 1000 AS k, number AS v FROM numbers(300000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT number % 1000 AS k, number AS v FROM numbers(300000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT 'dups 40000 one key, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT 7 AS k, number AS v FROM numbers(40000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN (SELECT 7 AS k, number AS v FROM numbers(40000)) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT '32767-row key, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN t_sc_sat AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_z AS p INNER JOIN t_sc_sat AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

SELECT '32767-row key, partitioned_hash rows', count() FROM t_sc_z AS p INNER JOIN t_sc_sat AS b ON p.k = b.k
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0, log_comment = '05138_sat';

SELECT 'chains across 777-row blocks, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_b AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0, max_block_size = 777) AS q);

SELECT 'grouped scatter, inner all', h = q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_g AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sc_p AS p INNER JOIN t_sc_g AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0) AS q);

-- Measured band on this data was 70–90 MiB; pin the middle and assert ScatterGroups > 1, never an exact count.
SELECT 'grouped scatter in memory', count() > 0 FROM t_sc_p AS p INNER JOIN t_sc_g AS b ON p.k = b.k
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 80000000, max_threads = 8, max_block_size = 65536, max_insert_block_size = 65536, log_comment = '05138_grouped';

SYSTEM FLUSH LOGS query_log;

SELECT '32767-row key: exact span bytes',
    ProfileEvents['PartitionedHashJoinDuplicateRunBytes'] = 8 * 32767 + 16
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05138_sat'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'grouped scatter groups',
    countIf(ProfileEvents['PartitionedHashJoinScatterGroups'] > 1 AND ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05138_grouped';

DROP TABLE t_sc_b;
DROP TABLE t_sc_p;
DROP TABLE t_sc_sat;
DROP TABLE t_sc_z;
DROP TABLE t_sc_g;
