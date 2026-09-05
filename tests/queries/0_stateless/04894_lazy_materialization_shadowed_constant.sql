-- Without plan optimizations the chain below the sort re-materializes the constant of a constant
-- SELECT item while still receiving it as an input, so an `ExpressionStep` DAG holds a `COLUMN`
-- node named as one of its inputs. Splitting such a DAG for lazy materialization renamed the node
-- promoted to the lazy half, and the re-stacked `Sorting` step no longer resolved its sort
-- description: `Not found column 2_UInt8 in block ... 2_UInt8_0` (issue #112209).
-- Fixed by skipping the optimization for a DAG whose computed node shadows an input name.

DROP TABLE IF EXISTS t_04894;
CREATE TABLE t_04894 (v Int64, k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04894 SELECT number, number FROM numbers(100);

SET query_plan_enable_optimizations = 0, query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

SELECT 'constant item, folded conjunct';
SELECT v, 2 FROM (SELECT v, k FROM t_04894 WHERE v) AS sub
WHERE k = 5 AND (v BETWEEN 10 AND 5 AND 24576)
ORDER BY ALL LIMIT 6;

SELECT 'constant expression item';
SELECT v, 1 = 1 FROM (SELECT v, k FROM t_04894 WHERE v) AS sub
WHERE k = 5 AND (v BETWEEN 10 AND 5 AND 24576)
ORDER BY ALL LIMIT 6;

SELECT 'several constant items';
SELECT v, 2, 3, 'x' FROM (SELECT v, k FROM t_04894 WHERE v) AS sub
WHERE k = 5 AND (v BETWEEN 10 AND 5 AND 24576)
ORDER BY ALL LIMIT 6;

SELECT 'constant item, matching rows';
SELECT v, 2 FROM (SELECT v, k FROM t_04894 WHERE v) AS sub
WHERE k > 96
ORDER BY ALL LIMIT 6;

DROP TABLE t_04894;
