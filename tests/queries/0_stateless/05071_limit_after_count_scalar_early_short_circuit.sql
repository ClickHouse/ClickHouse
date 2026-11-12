-- A `count()` scalar subquery with a `LIMIT AFTER`/`UNTIL` boundary is not a plain count that early
-- short-circuiting may skip: the boundary is evaluated on the aggregated row and can fail, like `HAVING`.
DROP TABLE IF EXISTS t_scalar_range;
CREATE TABLE t_scalar_range (x UInt8) ENGINE = Memory;

SELECT 0 AND ((SELECT count() FROM t_scalar_range LIMIT AFTER intDiv(1, count()) > 0) = 0) SETTINGS enable_function_early_short_circuit = 1; -- { serverError ILLEGAL_DIVISION }
SELECT 0 AND ((SELECT count() FROM t_scalar_range LIMIT UNTIL intDiv(1, count()) > 0) = 0) SETTINGS enable_function_early_short_circuit = 1; -- { serverError ILLEGAL_DIVISION }
SELECT 0 AND ((SELECT count() FROM t_scalar_range HAVING intDiv(1, count()) > 0) = 0) SETTINGS enable_function_early_short_circuit = 1; -- { serverError ILLEGAL_DIVISION }

DROP TABLE t_scalar_range;
