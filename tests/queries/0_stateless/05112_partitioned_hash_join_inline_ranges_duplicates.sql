-- Duplicate-heavy and skewed builds under `join_algorithm = 'partitioned_hash'` against `hash`: one key
-- with more rows than a cell word can count (the run-list descriptor path), a Zipf-like skew where a few
-- keys hold most rows, duplicates of a key spread over many small build blocks, and several thread counts.
-- Each line prints the two results and whether they are equal.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_ird_build;
DROP TABLE IF EXISTS t_ird_probe;
DROP TABLE IF EXISTS t_ird_zipf;

-- 400000 rows: key 7 takes every tenth row (40000 of them, above the 32767 the cell word counts), the
-- other 60000 keys about six rows each.
CREATE TABLE t_ird_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT if(number % 10 = 0, 7, number % 60000) AS k, number AS v FROM numbers(400000);

CREATE TABLE t_ird_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, number AS p FROM numbers(70000);

-- A heavily skewed build: 300000 rows over 540 keys (the cube of a uniform variate, scaled), from about
-- 185 rows per key at the tail to about 37000 at key 0, so every key is a long run or a run list.
CREATE TABLE t_ird_zipf ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt64(floor(pow((number % 1000000) / 1000000., 3) * 20000)) AS k, number AS v FROM numbers(300000);

SELECT 'inner all, saturated key', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q)
SETTINGS log_comment = '05112 saturated';

SELECT 'right all, saturated key', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right any, saturated key', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ird_probe AS p ANY RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ird_probe AS p ANY RIGHT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner any, saturated key', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ird_probe AS p ANY INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ird_probe AS p ANY INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- Small build blocks: a key's rows arrive in many different blocks and, on a grouped build, groups.
SELECT 'inner all, duplicates across 777-row blocks', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_block_size = 777) AS q);

SELECT 'full all, duplicates across 777-row blocks, max_threads 1', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p FULL JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p FULL JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_block_size = 777, max_threads = 1) AS q);

SELECT 'left all, max_threads 16', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p LEFT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p LEFT JOIN t_ird_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS q);

SELECT 'inner all, zipf', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q)
SETTINGS log_comment = '05112 zipf';

SELECT 'right all, zipf', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ird_probe AS p RIGHT JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- A skewed probe over the skewed build: every thousandth build row probes back, so the hot keys are
-- probed many times and each hit emits thousands of rows.
SELECT 'inner all, zipf build probed by a zipf sample', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.v, b.v))) FROM (SELECT * FROM t_ird_zipf WHERE v % 1000 = 0) AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.v, b.v))) FROM (SELECT * FROM t_ird_zipf WHERE v % 1000 = 0) AS p INNER JOIN t_ird_zipf AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- A grouped post-build scatter (a memory budget between the grouped floor and the ungrouped peak of this
-- build, about 81 and 96 million bytes) with a hot key of about 40000 rows: every group appends its rows of
-- the hot key as one more node of the key's run list, and the probe walks the nodes in group order. The two
-- keys pack into `keys128`. Two budgets, so a small drift of the byte accounting cannot make both miss.
SELECT 'inner all, grouped scatter with a hot key, budget 84 MB', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.v, b.k))) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT if(number % 37 = 0, 7, number % 300000) AS k, if(number % 37 = 0, 8, (number % 300000) + 1) AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_threads = 8, max_block_size = 4096) AS h,
    (SELECT (count(), sum(cityHash64(p.v, b.k))) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT if(number % 37 = 0, 7, number % 300000) AS k, if(number % 37 = 0, 8, (number % 300000) + 1) AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_threads = 8, max_block_size = 4096, max_bytes_before_external_join = 84000000) AS q)
SETTINGS log_comment = '05112 grouped hot key';

SELECT 'inner all, grouped scatter with a hot key, budget 86 MB', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.v, b.k))) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT if(number % 37 = 0, 7, number % 300000) AS k, if(number % 37 = 0, 8, (number % 300000) + 1) AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_threads = 8, max_block_size = 4096) AS h,
    (SELECT (count(), sum(cityHash64(p.v, b.k))) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT if(number % 37 = 0, 7, number % 300000) AS k, if(number % 37 = 0, 8, (number % 300000) + 1) AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_threads = 8, max_block_size = 4096, max_bytes_before_external_join = 86000000) AS q)
SETTINGS log_comment = '05112 grouped hot key';

SYSTEM FLUSH LOGS query_log;

SELECT
    'grouped hot key: at least one build grouped in memory',
    countIf(ProfileEvents['PartitionedHashJoinScatterGroups'] > 1 AND ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112 grouped hot key' AND type = 'QueryFinish';

SELECT
    'saturated build engaged',
    ProfileEvents['PartitionedHashJoinLeafRows'] = (SELECT count() FROM t_ird_build),
    ProfileEvents['PartitionedHashJoinDuplicateRunBytes'] >= 8 * (SELECT count() FROM t_ird_build WHERE k = 7)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112 saturated' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT
    'zipf build engaged',
    ProfileEvents['PartitionedHashJoinLeafRows'] = (SELECT count() FROM t_ird_zipf),
    ProfileEvents['PartitionedHashJoinDuplicateRunBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112 zipf' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_ird_build;
DROP TABLE t_ird_probe;
DROP TABLE t_ird_zipf;
