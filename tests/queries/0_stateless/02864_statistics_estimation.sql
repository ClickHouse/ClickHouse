-- Tags: no-fasttest

DROP TABLE IF EXISTS tab SYNC;
DROP TABLE IF EXISTS tab2 SYNC;

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
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d = cast(1, 'DateTime')/*100*/ and c = 0/*1000*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
ALTER TABLE tab DROP STATISTICS a, b, c, d;


SELECT 'Test statistics multi-types:';

ALTER TABLE tab ADD STATISTICS a TYPE count_min, uniq;
ALTER TABLE tab ADD STATISTICS b, c, d TYPE count_min, minmax, uniq, tdigest;
ALTER TABLE tab MATERIALIZE STATISTICS a, b, c, d;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d = cast(1, 'DateTime')/*1*/ and c < -90/*900*/ and b > 900/*990*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8|_UInt16|_String|_DateTime', '')
FROM (EXPLAIN actions=1 SELECT count(*) FROM tab WHERE d > cast(1, 'DateTime')/*9999*/ and c < 0/*9900*/ and b = 0/*10*/ and a = '10000'/*0*/)
WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE IF EXISTS tab SYNC;

SELECT 'Test statistics implicitly type conversion:';

CREATE TABLE tab2
(
    a String,
    b UInt64,
    c UInt8,
    d DateTime,
    e Boolean,
    f Float64,
    g Decimal32(1),
    pk String,
) Engine = MergeTree() ORDER BY pk;

ALTER TABLE tab2 ADD STATISTICS a TYPE count_min, uniq;
ALTER TABLE tab2 ADD STATISTICS b, c, d, e, f, g TYPE count_min, minmax, uniq, tdigest;

INSERT INTO tab2 select toString(number), number, number, cast(number, 'DateTime'), number % 2, number, toDecimal32(number, 1), toString(number) FROM system.numbers LIMIT 100;

SELECT count(*) FROM tab2 WHERE a = '0';
SELECT count(*) FROM tab2 WHERE a = 0; -- { serverError NO_COMMON_TYPE }

SELECT count(*) FROM tab2 WHERE b = 1.1;

SELECT count(*) FROM tab2 WHERE c = 1.1;
SELECT count(*) FROM tab2 WHERE c = 1000; -- out of range of UInt16

SELECT count(*) FROM tab2 WHERE d = '2024-08-06 09:58:09';
SELECT count(*) FROM tab2 WHERE d = '2024-08-06 09:58:0';  -- { serverError CANNOT_PARSE_DATETIME }

SELECT count(*) FROM tab2 WHERE e = true;
SELECT count(*) FROM tab2 WHERE e = 'true'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab2 WHERE e = 1;
SELECT count(*) FROM tab2 WHERE e = 2;
SELECT count(*) FROM tab2 WHERE e = 1.1;
SELECT count(*) FROM tab2 WHERE e = '1';

SELECT count(*) FROM tab2 WHERE f = 1.1;
SELECT count(*) FROM tab2 WHERE f = '1.1';
SELECT count(*) FROM tab2 WHERE f = 1;
SELECT count(*) FROM tab2 WHERE f = '1';

SELECT count(*) FROM tab2 WHERE g = toDecimal32(1.0, 1);
SELECT count(*) FROM tab2 WHERE g = toDecimal32(1.10, 1);
SELECT count(*) FROM tab2 WHERE g = toDecimal32(1.0, 2);
SELECT count(*) FROM tab2 WHERE g = 1.0;
SELECT count(*) FROM tab2 WHERE g = 1.0;
SELECT count(*) FROM tab2 WHERE g = '1.0';

DROP TABLE IF EXISTS tab2 SYNC;
