-- Coverage for float-to-Decimal rounding (round half to even) across the cast entry points and
-- code paths that the dedicated 04034 test does not exercise:
--   1. the compiled-expression path,
--   2. the accurateCast family,
--   3. INSERT-time type conversion,
--   4. the wide Decimal128/Decimal256 targets,
--   5. non-finite (inf/nan) inputs.

SELECT '1. compile_expressions path';
-- Float -> Decimal JIT is intentionally disabled (it would lower to a poison FPToSI), so this must
-- fall back to the interpreted cast: ties still round to even, and overflow still throws (no UB).
SELECT CAST(arrayJoin([0.5, 1.5, 2.5, 3.5]::Array(Float64)), 'Decimal64(0)') AS d
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT CAST(materialize(1e30::Float64), 'Decimal64(0)')
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError DECIMAL_OVERFLOW }

SELECT '2. accurateCast from Float';
-- accurateCast rounds (banker's) and honors the compatibility setting.
SELECT accurateCast(1.5::Float64, 'Decimal64(0)') AS round_default;
SELECT accurateCast(1.5::Float64, 'Decimal64(0)') AS truncated SETTINGS cast_float_to_decimal_uses_rounding = 0;
SELECT accurateCast(2.5::Float64, 'Decimal64(0)'), accurateCast(0.5::Float64, 'Decimal64(0)');
-- OrNull / OrDefault on non-finite input; plain accurateCast on non-finite must throw.
SELECT accurateCastOrNull(nan::Float64, 'Decimal64(2)'), accurateCastOrNull(inf::Float64, 'Decimal64(2)');
SELECT accurateCastOrDefault(inf::Float64, 'Decimal64(2)'), accurateCastOrDefault(16.4::Float64, 'Decimal64(6)');
SELECT accurateCast(inf::Float64, 'Decimal64(2)'); -- { serverError DECIMAL_OVERFLOW }

SELECT '3. INSERT float into Decimal column';
DROP TABLE IF EXISTS t_dec_round;
CREATE TABLE t_dec_round (d Decimal64(0)) ENGINE = Memory;
INSERT INTO t_dec_round VALUES (0.5::Float64), (1.5::Float64), (2.5::Float64), (3.5::Float64);
SELECT 'insert rounds:', groupArray(d) FROM (SELECT d FROM t_dec_round ORDER BY d);
TRUNCATE TABLE t_dec_round;
INSERT INTO t_dec_round SETTINGS cast_float_to_decimal_uses_rounding = 0 VALUES (0.5::Float64), (1.5::Float64), (2.5::Float64), (3.5::Float64);
SELECT 'insert truncates:', groupArray(d) FROM (SELECT d FROM t_dec_round ORDER BY d);
DROP TABLE t_dec_round;

SELECT '4. wide decimals (Decimal128/Decimal256) ties';
SELECT toDecimal128(0.5, 0), toDecimal128(1.5, 0), toDecimal128(2.5, 0), toDecimal128(3.5, 0);
SELECT toDecimal256(0.5, 0), toDecimal256(1.5, 0), toDecimal256(2.5, 0), toDecimal256(3.5, 0);

SELECT '5. inf / nan';
SELECT toDecimal64(inf, 2);  -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(-inf, 2); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(nan, 2);  -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(materialize(inf::Float64), 'Decimal64(2)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(materialize(nan::Float64), 'Decimal64(2)'); -- { serverError DECIMAL_OVERFLOW }
