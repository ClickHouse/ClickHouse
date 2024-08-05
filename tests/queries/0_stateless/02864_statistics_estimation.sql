-- Tags: no-fasttest

DROP TABLE IF EXISTS tab SYNC;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET allow_suspicious_low_cardinality_types=1;
SET mutations_sync = 2;

CREATE TABLE tab
(
    a String,
    b UInt64,
    c Int64,
    d DateTime,
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE tab;

INSERT INTO tab select toString(number % 10000), number % 1000, -(number % 100), cast(number, 'DateTime'), generateUUIDv4() FROM system.numbers LIMIT 10000;


SELECT 'Test statistics count_min:';

ALTER TABLE tab ADD STATISTICS a, b, c TYPE count_min;
ALTER TABLE tab MATERIALIZE STATISTICS a, b, c;
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE c = 0/*100*/ and b = 0/*10*/ and a = '0'/*1*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
ALTER TABLE tab DROP STATISTICS a, b, c;


SELECT 'Test statistics minmax:';

ALTER TABLE tab ADD STATISTICS b, c, d TYPE minmax;
ALTER TABLE tab MATERIALIZE STATISTICS b, c, d;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b > 0/*10000*/ and c < -1/*9990*/ and d > cast(9998, 'DateTime')/*1*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE tab DROP STATISTICS b, c, d;


SELECT 'Test statistics tdigest:';

ALTER TABLE tab ADD STATISTICS b, c, d TYPE tdigest;
ALTER TABLE tab MATERIALIZE STATISTICS b, c, d;
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE b > 0/*10000*/ and c < -1/*9990*/ and d > cast(9998, 'DateTime')/*1*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
ALTER TABLE tab DROP STATISTICS b, c, d;


SELECT 'Test statistics uniq:';

ALTER TABLE tab ADD STATISTICS a, b, c, d TYPE uniq;
ALTER TABLE tab MATERIALIZE STATISTICS a, b, c, d;
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d = cast(1, 'DateTime')/*100*/ and c = 0/*1000*/ and b = 0/*100*/ and a = '0'/*100*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
ALTER TABLE tab DROP STATISTICS a, b, c, d;


SELECT 'Test statistics multi-types:';

ALTER TABLE tab ADD STATISTICS a TYPE count_min, uniq;
ALTER TABLE tab ADD STATISTICS b TYPE count_min, minmax, uniq, tdigest;
ALTER TABLE tab ADD STATISTICS c TYPE count_min, minmax, uniq, tdigest;
ALTER TABLE tab ADD STATISTICS d TYPE count_min, minmax, uniq, tdigest;
ALTER TABLE tab MATERIALIZE STATISTICS a, b, c, d;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d = cast(1, 'DateTime')/*1*/ and c < -90/*900*/ and b > 900/*990*/ and a = '0'/*1*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d > cast(1, 'DateTime')/*9999*/ and c < 0/*9900*/ and b = 0/*10*/ and a = '10000'/*0*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

ALTER TABLE tab DROP STATISTICS a, b, c, d;


DROP TABLE IF EXISTS tab SYNC;
