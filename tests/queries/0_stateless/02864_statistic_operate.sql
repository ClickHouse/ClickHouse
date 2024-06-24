DROP TABLE IF EXISTS t1;

SET allow_experimental_statistic = 1;
SET allow_statistic_optimize = 1;

CREATE TABLE t1
(
    a Float64 STATISTIC(tdigest),
    b Int64 STATISTIC(tdigest),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE t1;

INSERT INTO t1 select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After insert';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;
SELECT count(*) FROM t1 WHERE b < NULL and a < '10';

ALTER TABLE t1 DROP STATISTIC a, b TYPE tdigest;

SELECT 'After drop statistic';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

SHOW CREATE TABLE t1;

ALTER TABLE t1 ADD STATISTIC a, b TYPE tdigest;

SELECT 'After add statistic';

SHOW CREATE TABLE t1;

ALTER TABLE t1 MATERIALIZE STATISTIC a, b TYPE tdigest;
INSERT INTO t1 select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After materialize statistic';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

OPTIMIZE TABLE t1 FINAL;

SELECT 'After merge';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

ALTER TABLE t1 RENAME COLUMN b TO c;
SHOW CREATE TABLE t1;

SELECT 'After rename';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM t1 WHERE c < 10 and a < 10;

DROP TABLE IF EXISTS t1;
