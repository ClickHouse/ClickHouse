-- Tests JIT compilation of expression functions on Decimal types.
-- PR #88770 added JIT for conversions, abs, sign on Decimals but had no tests.

SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_jit_dec_expr;
CREATE TABLE test_jit_dec_expr
(
    d32 Decimal32(4),
    d64 Decimal64(8),
    d128 Decimal128(18),
    i64 Int64,
    f64 Float64
) ENGINE = TinyLog;

INSERT INTO test_jit_dec_expr VALUES (-5.1234, -123.45678901, -999.123456789012345678, -42, -3.14);
INSERT INTO test_jit_dec_expr VALUES (0, 0, 0, 0, 0);
INSERT INTO test_jit_dec_expr VALUES (7.9999, 999.99999999, 999.999999999999999999, 100, 2.718);
INSERT INTO test_jit_dec_expr VALUES (-0.0001, -0.00000001, -0.000000000000000001, -1, -0.001);

SELECT 'Test abs on Decimal types';
SELECT abs(d32), abs(d64), abs(d128) FROM test_jit_dec_expr ORDER BY d32;

SELECT 'Test sign on Decimal types';
SELECT sign(d32), sign(d64), sign(d128) FROM test_jit_dec_expr ORDER BY d32;

SELECT 'Test Decimal to integer conversions';
SELECT toInt32(d32), toInt64(d64), toInt64(d128) FROM test_jit_dec_expr ORDER BY d32;

SELECT 'Test Decimal to float conversions';
SELECT toFloat32(d32), toFloat64(d64) FROM test_jit_dec_expr ORDER BY d32;

SELECT 'Test integer to Decimal conversions';
SELECT toDecimal32(i64, 2), toDecimal64(i64, 4) FROM test_jit_dec_expr ORDER BY i64;

SELECT 'Test float to Decimal conversions';
SELECT toDecimal64(f64, 4) FROM test_jit_dec_expr ORDER BY f64;

SELECT 'Test Decimal to Decimal conversions (different scales)';
SELECT toDecimal64(d32, 8), toDecimal32(d64, 4) FROM test_jit_dec_expr ORDER BY d32;

SELECT 'Test isNull/isNotNull with Nullable(Decimal)';

DROP TABLE IF EXISTS test_jit_dec_null_expr;
CREATE TABLE test_jit_dec_null_expr (val Nullable(Decimal64(4))) ENGINE = TinyLog;
INSERT INTO test_jit_dec_null_expr VALUES (1.5), (NULL), (-3.0), (NULL), (0.0);
SELECT val, isNull(val), isNotNull(val) FROM test_jit_dec_null_expr ORDER BY val NULLS LAST;

SELECT 'Test assumeNotNull with Nullable(Decimal)';
SELECT assumeNotNull(val) FROM test_jit_dec_null_expr WHERE val IS NOT NULL ORDER BY val;

DROP TABLE test_jit_dec_null_expr;
DROP TABLE test_jit_dec_expr;
