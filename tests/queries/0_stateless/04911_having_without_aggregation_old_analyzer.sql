CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t SELECT number + 1 FROM numbers(3);

SELECT '-- no aggregation, predicate rewrite off';
SELECT a FROM t HAVING a > 999 ORDER BY a
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- no aggregation, stateful conjunct blocks the rewrite at default settings';
SELECT a FROM t HAVING a > 999 AND blockNumber() >= 0 ORDER BY a
SETTINGS enable_analyzer = 0;

SELECT '-- WHERE and HAVING are combined, not replaced';
SELECT a FROM t WHERE a > 1 HAVING a < 3 ORDER BY a
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- an alias in HAVING resolves to the projection';
SELECT a * 2 AS d FROM t HAVING d > 4 ORDER BY d
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- rowNumberInBlock is numbered over the unfiltered block, since both conditions merge into one filter';
SELECT number FROM numbers(10) WHERE number % 2 = 0 HAVING rowNumberInBlock() = 1 ORDER BY number
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- the same predicate matches the row at the unfiltered index';
SELECT number FROM numbers(10) WHERE number % 2 = 0 HAVING rowNumberInBlock() = 2 ORDER BY number
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- the predicate reaches the storage and prunes';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM t HAVING a > 999) WHERE explain ILIKE '%Parts: 0/1%'
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- aggregation keeps HAVING in the aggregation branch';
SELECT a FROM t GROUP BY a HAVING a > 999 ORDER BY a
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- a bare column with an aggregate HAVING is still rejected';
SELECT a FROM t HAVING count() > 999
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0; -- { serverError NOT_AN_AGGREGATE }

SELECT '-- a window function in HAVING is still rejected';
SELECT a FROM t HAVING count() OVER () > 999
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0; -- { serverError ILLEGAL_AGGREGATION }

SELECT '-- WITH TOTALS without aggregation is still rejected';
SELECT a FROM t WITH TOTALS HAVING a > 999
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0; -- { serverError NOT_IMPLEMENTED }

SELECT '-- the predicate is applied on shards too';
SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t) HAVING a > 2 ORDER BY a
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

DROP TABLE t;
