-- Tests that various DDL statements create/drop/materialize statistics

DROP TABLE IF EXISTS tab;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

CREATE TABLE tab
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE tab;

INSERT INTO tab select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After insert';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM tab WHERE b < 10 and a < 10;
SELECT count(*) FROM tab WHERE b < NULL and a < '10';

ALTER TABLE tab DROP STATISTICS a, b;

SELECT 'After drop statistic';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM tab WHERE b < 10 and a < 10;

SHOW CREATE TABLE tab;

ALTER TABLE tab ADD STATISTICS a, b TYPE tdigest;

SELECT 'After add statistic';

SHOW CREATE TABLE tab;

ALTER TABLE tab MATERIALIZE STATISTICS a, b;
INSERT INTO tab select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After materialize statistic';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM tab WHERE b < 10 and a < 10;

OPTIMIZE TABLE tab FINAL;

SELECT 'After merge';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM tab WHERE b < 10 and a < 10;

ALTER TABLE tab RENAME COLUMN b TO c;
SHOW CREATE TABLE tab;

SELECT 'After rename';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE c < 10 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT count(*) FROM tab WHERE c < 10 and a < 10;

DROP TABLE IF EXISTS tab;
