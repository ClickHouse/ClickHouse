-- https://github.com/ClickHouse/ClickHouse/issues/101540
-- JIT-compiled `intExp2` must match the non-JIT path: clamp shifts `>= 64` to `UINT64_MAX`,
-- return `0` for negative inputs, and preserve exceptions for `NaN` / big-integer inputs.
-- Forcing JIT requires a compilable subexpression in the argument (e.g. `toInt32(number + k)`).

SELECT '== positive overflow, Int32 ==';
SELECT intExp2(toInt32(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 0;
SELECT intExp2(toInt32(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== negative, Int8 ==';
SELECT intExp2(toInt8(number - 5)) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toInt8(number - 5)) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== negative, Int64 ==';
SELECT intExp2(toInt64(number) - 5) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toInt64(number) - 5) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== UInt8 boundary 63..67 ==';
SELECT intExp2(toUInt8(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 0;
SELECT intExp2(toUInt8(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== UInt64 boundary 63..67 ==';
SELECT intExp2(toUInt64(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 0;
SELECT intExp2(toUInt64(number + 63)) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Inputs whose magnitude exceeds INT_MAX previously hit implementation-defined narrowing
-- via `static_cast<int>` in the non-JIT path and silently returned bogus values.
SELECT '== Int64 magnitude > INT_MAX ==';
SELECT intExp2(toInt64(number) + 9223372036854775807) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toInt64(number) + 9223372036854775807) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT intExp2(toInt64(number) - 9223372036854775807) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toInt64(number) - 9223372036854775807) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== UInt32 > 2^31 ==';
SELECT intExp2(toUInt32(number + 4294967290)) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toUInt32(number + 4294967290)) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== UInt64 > 2^32 ==';
SELECT intExp2(toUInt64(number) + toUInt64(18446744073709551611)) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toUInt64(number) + toUInt64(18446744073709551611)) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== Float32 negative ==';
SELECT intExp2(toFloat32(number) - 5.0) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toFloat32(number) - 5.0) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== Float32 boundary 63..67 ==';
SELECT intExp2(toFloat32(number) + 63.0) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 0;
SELECT intExp2(toFloat32(number) + 63.0) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== Float64 negative ==';
SELECT intExp2(toFloat64(number) - 5.0) FROM numbers(1) SETTINGS compile_expressions = 0;
SELECT intExp2(toFloat64(number) - 5.0) FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '== Float64 boundary 63..67 ==';
SELECT intExp2(toFloat64(number) + 63.0) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 0;
SELECT intExp2(toFloat64(number) + 63.0) FROM numbers(5) ORDER BY number SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- `BFloat16` is not in `FunctionUnaryArithmetic::castType` (`intExp2(BFloat16)` is rejected as
-- `ILLEGAL_TYPE_OF_ARGUMENT`), so there's nothing to cover for that type here.

SELECT '== BFloat16 rejected ==';
SELECT intExp2(toBFloat16(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '== Float32 NaN throws ==';
SELECT intExp2(toFloat32(nan)) SETTINGS compile_expressions = 0; -- { serverError BAD_ARGUMENTS }
SELECT intExp2(toFloat32(nan)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError BAD_ARGUMENTS }

SELECT '== Float64 NaN throws ==';
SELECT intExp2(nan) SETTINGS compile_expressions = 0; -- { serverError BAD_ARGUMENTS }
SELECT intExp2(nan) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError BAD_ARGUMENTS }

SELECT '== Int128 not implemented ==';
SELECT intExp2(toInt128(70)) SETTINGS compile_expressions = 0; -- { serverError NOT_IMPLEMENTED }
SELECT intExp2(toInt128(70)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }

SELECT '== UInt128 not implemented ==';
SELECT intExp2(toUInt128(70)) SETTINGS compile_expressions = 0; -- { serverError NOT_IMPLEMENTED }
SELECT intExp2(toUInt128(70)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
