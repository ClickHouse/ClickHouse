DROP TABLE IF EXISTS t1;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

CREATE TABLE t1
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest),
    c Int64 STATISTICS(tdigest, uniq),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;

SHOW CREATE TABLE t1;

INSERT INTO t1 select number, -number, number/1000, generateUUIDv4() FROM system.numbers LIMIT 10000;
INSERT INTO t1 select 0, 0, 11, generateUUIDv4();

SELECT 'After insert';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 0 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 11 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
OPTIMIZE TABLE t1 FINAL;

SELECT 'After merge';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 0 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 11 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

SELECT 'After modify TDigest';
ALTER TABLE t1 MODIFY STATISTICS c TYPE TDigest;
ALTER TABLE t1 MATERIALIZE STATISTICS c;

SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 11 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 0 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c < -1 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';


ALTER TABLE t1 DROP STATISTICS c;

SELECT 'After drop';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 11 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c = 0 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1.|_UInt8|_Int8', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM t1 WHERE b < 10 and c < -1 and a < 10) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE t2
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest),
    c LowCardinality(Int64) STATISTICS(tdigest, uniq),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t2 select number, -number, number/1000, generateUUIDv4() FROM system.numbers LIMIT 10000;

DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t3
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest),
    c Nullable(Int64) STATISTICS(tdigest, uniq),
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t3 select number, -number, number/1000, generateUUIDv4() FROM system.numbers LIMIT 10000;

DROP TABLE IF EXISTS t3;
