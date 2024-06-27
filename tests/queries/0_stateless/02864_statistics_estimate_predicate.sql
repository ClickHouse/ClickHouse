-- Tags: no-fasttest
DROP TABLE IF EXISTS t1 SYNC;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET allow_suspicious_low_cardinality_types=1;
SET mutations_sync = 2;

CREATE TABLE t1
(
    a String,
    b UInt64,
    c Int64,
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE t1;

INSERT INTO t1 select toString(number % 10000), number % 1000, -(number % 100), generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'Test statistics TDigest:';

ALTER TABLE t1 ADD STATISTICS b, c TYPE tdigest;
ALTER TABLE t1 MATERIALIZE STATISTICS b, c;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b > 0/*9990*/ and c < -98/*100*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b = 0/*1000*/ and c < -98/*100*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE t1 DROP STATISTICS b, c;


SELECT 'Test statistics Uniq:';

ALTER TABLE t1 ADD STATISTICS b TYPE uniq, tdigest;
ALTER TABLE t1 MATERIALIZE STATISTICS b;
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0/*1000*/ and b = 0/*10*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE t1 DROP STATISTICS b;


SELECT 'Test statistics count_min:';

ALTER TABLE t1 ADD STATISTICS a TYPE count_min;
ALTER TABLE t1 ADD STATISTICS b TYPE count_min;
ALTER TABLE t1 ADD STATISTICS c TYPE count_min;
ALTER TABLE t1 MATERIALIZE STATISTICS a, b, c;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c = 0/*100*/ and b = 0/*10*/ and a = '0'/*1*/) xx
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE t1 DROP STATISTICS a, b, c;


SELECT 'Test statistics multi-types:';

ALTER TABLE t1 ADD STATISTICS a TYPE count_min;
ALTER TABLE t1 ADD STATISTICS b TYPE count_min, uniq, tdigest;
ALTER TABLE t1 ADD STATISTICS c TYPE count_min, uniq, tdigest;
ALTER TABLE t1 MATERIALIZE STATISTICS a, b, c;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c < -90/*900*/ and b > 900/*990*/ and a = '0'/*1*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_String', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE c < 0/*9900*/ and b = 0/*10*/ and a = '10000'/*0*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE t1 DROP STATISTICS a, b, c;

DROP TABLE IF EXISTS t1 SYNC;


SELECT 'Test LowCardinality and Nullable data type:';
DROP TABLE IF EXISTS t2 SYNC;
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE t2
(
    a LowCardinality(Int64) STATISTICS(uniq, tdigest, count_min),
    b Nullable(Int64) STATISTICS(uniq, tdigest, count_min),
    c LowCardinality(Nullable(Int64)) STATISTICS(uniq, tdigest, count_min),
    pk String,
) Engine = MergeTree() ORDER BY pk;

select table from system.tables where name = 't2';

DROP TABLE IF EXISTS t2 SYNC;
