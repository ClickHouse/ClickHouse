-- The test harness may randomize these settings, and the EXPLAIN check below depends on them.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_memory_prewhere;
DROP TABLE IF EXISTS t_memory_prewhere_compressed;

CREATE TABLE t_memory_prewhere (k UInt64, s String, v UInt64) ENGINE = Memory;
CREATE TABLE t_memory_prewhere_compressed (k UInt64, s String, v UInt64) ENGINE = Memory SETTINGS compress = 1;

-- Several inserts, so the tables consist of multiple blocks, including blocks
-- that are fully eliminated by the conditions below.
INSERT INTO t_memory_prewhere SELECT number, concat('str', toString(number)), number * 2 FROM numbers(0, 100);
INSERT INTO t_memory_prewhere SELECT number, concat('str', toString(number)), number * 2 FROM numbers(100, 100);
INSERT INTO t_memory_prewhere SELECT number, concat('str', toString(number)), number * 2 FROM numbers(200, 100);
INSERT INTO t_memory_prewhere_compressed SELECT * FROM t_memory_prewhere ORDER BY k LIMIT 100;
INSERT INTO t_memory_prewhere_compressed SELECT * FROM t_memory_prewhere ORDER BY k LIMIT 100 OFFSET 100;
INSERT INTO t_memory_prewhere_compressed SELECT * FROM t_memory_prewhere ORDER BY k LIMIT 100 OFFSET 200;

SELECT 'explicit PREWHERE';
SELECT k, s, v FROM t_memory_prewhere PREWHERE k = 105;
SELECT k, s, v FROM t_memory_prewhere_compressed PREWHERE k = 105;
SELECT s FROM t_memory_prewhere_compressed PREWHERE k % 100 = 7 ORDER BY s;

SELECT 'PREWHERE keeping the condition column';
SELECT k FROM t_memory_prewhere PREWHERE k = 105;
SELECT k, v FROM t_memory_prewhere_compressed PREWHERE k = 105 AND v = 210;

SELECT 'PREWHERE eliminating everything';
SELECT k, v FROM t_memory_prewhere PREWHERE k = 1000000;
SELECT count() FROM t_memory_prewhere_compressed PREWHERE s = 'no such value';

SELECT 'WHERE is moved to PREWHERE';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT v FROM t_memory_prewhere_compressed WHERE k = 105) WHERE explain LIKE '%Prewhere filter column%';
SELECT v FROM t_memory_prewhere_compressed WHERE k = 105;
SELECT v FROM t_memory_prewhere_compressed WHERE k = 105 SETTINGS optimize_move_to_prewhere = 0;

SELECT 'aggregation over PREWHERE';
SELECT sum(v), count() FROM t_memory_prewhere WHERE k >= 150 AND k < 250 AND s != '';
SELECT sum(v), count() FROM t_memory_prewhere_compressed WHERE k >= 150 AND k < 250 AND s != '';

SELECT 'old analyzer';
SELECT v FROM t_memory_prewhere PREWHERE k = 105 SETTINGS enable_analyzer = 0;
SELECT sum(v) FROM t_memory_prewhere_compressed PREWHERE k >= 150 AND k < 250 SETTINGS enable_analyzer = 0;

SELECT 'column added by ALTER';
ALTER TABLE t_memory_prewhere_compressed ADD COLUMN added UInt64;
INSERT INTO t_memory_prewhere_compressed SELECT number, concat('str', toString(number)), number * 2, 8 FROM numbers(300, 100);
-- The added column as the PREWHERE condition: rows of the old blocks get the default value.
SELECT count() FROM t_memory_prewhere_compressed PREWHERE added = 0;
SELECT count() FROM t_memory_prewhere_compressed PREWHERE added = 8;
-- The added column is read after filtering on another column.
SELECT added FROM t_memory_prewhere_compressed PREWHERE k = 105;
SELECT added FROM t_memory_prewhere_compressed PREWHERE k = 305;

SELECT 'subcolumns';
DROP TABLE IF EXISTS t_memory_prewhere_tuple;
CREATE TABLE t_memory_prewhere_tuple (t Tuple(a UInt64, b String), v UInt64) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_prewhere_tuple SELECT (number, concat('str', toString(number))), number * 2 FROM numbers(0, 100);
INSERT INTO t_memory_prewhere_tuple SELECT (number, concat('str', toString(number))), number * 2 FROM numbers(100, 100);
SELECT v FROM t_memory_prewhere_tuple PREWHERE t.a = 105;
SELECT t.b FROM t_memory_prewhere_tuple WHERE t.a = 105;

SELECT 'Nullable and LowCardinality';
DROP TABLE IF EXISTS t_memory_prewhere_null;
CREATE TABLE t_memory_prewhere_null (k UInt64, n Nullable(UInt64), lc LowCardinality(String)) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_prewhere_null SELECT number, if(number % 3 = 0, NULL, number), concat('lc', toString(number % 10)) FROM numbers(0, 100);
INSERT INTO t_memory_prewhere_null SELECT number, if(number % 3 = 0, NULL, number), concat('lc', toString(number % 10)) FROM numbers(100, 100);
SELECT count() FROM t_memory_prewhere_null PREWHERE n > 150;
SELECT count() FROM t_memory_prewhere_null PREWHERE n IS NULL;
SELECT count() FROM t_memory_prewhere_null WHERE lc = 'lc7';
SELECT k, n FROM t_memory_prewhere_null PREWHERE lc = 'lc7' AND n < 10;

SELECT 'empty table';
DROP TABLE IF EXISTS t_memory_prewhere_empty;
CREATE TABLE t_memory_prewhere_empty (k UInt64, v UInt64) ENGINE = Memory SETTINGS compress = 1;
SELECT v FROM t_memory_prewhere_empty PREWHERE k = 1;
SELECT count() FROM t_memory_prewhere_empty WHERE k = 1;

SELECT 'mutations still work';
ALTER TABLE t_memory_prewhere_compressed UPDATE v = v + 1 WHERE k = 105;
SELECT k, v FROM t_memory_prewhere_compressed PREWHERE k = 105;
ALTER TABLE t_memory_prewhere_compressed DELETE WHERE k = 105;
SELECT count() FROM t_memory_prewhere_compressed PREWHERE k = 105;
SELECT count() FROM t_memory_prewhere_compressed;

DROP TABLE t_memory_prewhere;
DROP TABLE t_memory_prewhere_compressed;
DROP TABLE t_memory_prewhere_tuple;
DROP TABLE t_memory_prewhere_null;
DROP TABLE t_memory_prewhere_empty;
