-- https://github.com/ClickHouse/ClickHouse/issues/117442
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_float_cast;
CREATE TABLE t_jit_float_cast (c0 UInt8) ENGINE = Memory;
INSERT INTO t_jit_float_cast VALUES (230), (250);

-- In-range conversions are exact and identical compiled or interpreted.
SELECT c0,
       toFloat64(c0) / 10 <= CAST(toFloat64(c0) AS UInt8) AS in_range_by_cast,
       CAST(toFloat64(c0) AS Decimal32(2))                AS in_range_decimal
FROM t_jit_float_cast
ORDER BY c0;

-- A value the destination cannot hold raises. The non-finite one is built from the column so that
-- it is not constant folded before execution.
SELECT CAST(-toFloat64(c0) * 1e9 AS Decimal32(2)) FROM t_jit_float_cast; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(-toFloat64(c0) * 1e9, 2) FROM t_jit_float_cast; -- { serverError DECIMAL_OVERFLOW }
SELECT toUInt8(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0))) FROM t_jit_float_cast; -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0)) AS UInt8) FROM t_jit_float_cast; -- { serverError CANNOT_CONVERT_TYPE }

-- The value an out-of-range float converts to is not defined by the language, so compare the compiled
-- and the interpreted evaluation of the same expression instead of pinning a literal.
CREATE TABLE t_jit_float_cast_arms (c0 UInt8, lte UInt8) ENGINE = Memory;

INSERT INTO t_jit_float_cast_arms
SELECT c0, toFloat64(c0) / 10 <= CAST(-toFloat64(c0) AS UInt8) FROM t_jit_float_cast;

SET compile_expressions = 0;
INSERT INTO t_jit_float_cast_arms
SELECT c0, toFloat64(c0) / 10 <= CAST(-toFloat64(c0) AS UInt8) FROM t_jit_float_cast;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT count() FROM (SELECT c0 FROM t_jit_float_cast_arms GROUP BY c0 HAVING uniqExact(lte) > 1);

DROP TABLE t_jit_float_cast_arms;

DROP TABLE t_jit_float_cast;
