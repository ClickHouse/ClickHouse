-- Tags: no-fasttest
DROP TABLE IF EXISTS t1;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

CREATE TABLE t1
(
    a String STATISTICS(cmsketch),
    b Int64 STATISTICS(cmsketch),
    c UInt64 STATISTICS(cmsketch),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE t1;

INSERT INTO t1 select toString(number % 1000), number % 100, number % 10, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0 and b = 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0 and b > 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE t1 DROP STATISTICS a;

SELECT 'After drop statistics for a';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0 and b = 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0 and b > 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE t2
(
    a LowCardinality(String) STATISTICS(cmsketch),
    b Int64 STATISTICS(cmsketch),
    c UInt64 STATISTICS(cmsketch),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t2 select toString(number % 1000), number % 100, number % 10, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'LowCardinality';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t2 WHERE c = 0 and b = 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';


DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t3
(
    a Nullable(String) STATISTICS(cmsketch),
    b Int64 STATISTICS(cmsketch),
    c UInt64 STATISTICS(cmsketch),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t3 select toString(number % 1000), number % 100, number % 10, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'Nullable';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t3 WHERE c = 0 and b = 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t4
(
    a LowCardinality(Nullable(String)) STATISTICS(cmsketch),
    b Int64 STATISTICS(cmsketch),
    c UInt64 STATISTICS(cmsketch),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t4 select toString(number % 1000), number % 100, number % 10, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'LowCardinality(Nullable)';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t4 WHERE c = 0 and b = 0 and a = '0') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE IF EXISTS t4;
