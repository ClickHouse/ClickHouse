SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_float_cast;
CREATE TABLE t_jit_float_cast (c0 UInt8) ENGINE = Memory;
INSERT INTO t_jit_float_cast VALUES (230), (250);

SELECT c0,
       cbrt(c0) <= CAST(-toFloat64(c0) AS UInt8) AS out_of_range_by_cast,
       cbrt(c0) <= toUInt8(-toFloat64(c0))       AS out_of_range_by_to_uint8,
       cbrt(c0) <= CAST(toFloat64(c0) AS UInt8)  AS in_range_by_cast,
       CAST(toFloat64(c0) AS Decimal32(2))       AS in_range_decimal
FROM t_jit_float_cast
ORDER BY c0;

SELECT CAST(-toFloat64(c0) * 1e9 AS Decimal32(2)) FROM t_jit_float_cast; -- { serverError DECIMAL_OVERFLOW }

DROP TABLE t_jit_float_cast;
