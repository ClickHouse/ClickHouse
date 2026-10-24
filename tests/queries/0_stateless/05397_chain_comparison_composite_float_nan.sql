-- Pruning of comparison conjuncts on the same expression must not order composite float constants
-- by `Field`, which places a nested NaN after all values, while `tup < (1, nan)` is false in execution.
-- Each optimized query is compared with the same predicate evaluated per row without the optimizer.

DROP TABLE IF EXISTS t_composite_float;
CREATE TABLE t_composite_float (arr Array(Float64), tup Tuple(Float64, Float64)) ENGINE = Memory;
INSERT INTO t_composite_float VALUES ([1.0], (1, nan)), ([nan], (1, 2)), ([2.0], (1, 1)), ([-0.0], (1, -0.0));

SELECT 'tuple equals and less';
SELECT count() FROM t_composite_float WHERE tup = (1, 1) AND tup < (1, nan);
SELECT sum((tup = (1, 1)) * (tup < (1, nan))) FROM t_composite_float;

SELECT 'tuple equals and greater';
SELECT count() FROM t_composite_float WHERE tup = (1, 2) AND tup > (1, nan);
SELECT sum((tup = (1, 2)) * (tup > (1, nan))) FROM t_composite_float;

SELECT 'tuple ranges';
SELECT count() FROM t_composite_float WHERE tup < (1, nan) AND tup < (1, 3);
SELECT sum((tup < (1, nan)) * (tup < (1, 3))) FROM t_composite_float;

SELECT 'tuple not equals';
SELECT count() FROM t_composite_float WHERE tup != (1, nan) AND tup != (1, 1);
SELECT sum((tup != (1, nan)) * (tup != (1, 1))) FROM t_composite_float;

SELECT 'tuple negative zero';
SELECT count() FROM t_composite_float WHERE tup = (1, 0.0) AND tup != (1, -0.0);
SELECT sum((tup = (1, 0.0)) * (tup != (1, -0.0))) FROM t_composite_float;

SELECT 'array equals and less';
SELECT count() FROM t_composite_float WHERE arr = [1.0] AND arr < [nan];
SELECT sum((arr = [1.0]) * (arr < [nan])) FROM t_composite_float;

SELECT 'array not equals';
SELECT count() FROM t_composite_float WHERE arr != [nan] AND arr != [1.0];
SELECT sum((arr != [nan]) * (arr != [1.0])) FROM t_composite_float;

SELECT 'array negative zero';
SELECT count() FROM t_composite_float WHERE arr = [0.0] AND arr != [-0.0];
SELECT sum((arr = [0.0]) * (arr != [-0.0])) FROM t_composite_float;

DROP TABLE t_composite_float;
