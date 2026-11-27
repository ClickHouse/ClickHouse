-- Test for JIT compilation of multiIf with mixed return types.
-- Exercises nativeCast between different integer widths (e.g., UInt8 -> UInt32)
-- and different float widths (Float32 -> Float64) inside JIT-compiled expressions.
-- Previously, nativeCast had copy-paste bugs where type checks on lines 203/205
-- checked the same variable twice instead of checking both from and to types.
-- Also exercises the PassBuilder fix that passes proper TargetMachine to LLVM.
-- https://github.com/ClickHouse/ClickHouse/issues/98580

SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

-- multiIf with mixed integer return types: UInt8, UInt16, UInt32
-- Result type is promoted to UInt32, so UInt8(3) and UInt16(2) need nativeCast to i32
SELECT multiIf(
    (number % 2) = 0, toUInt32(1),
    (number % 3) = 0, toUInt16(2),
    toUInt8(3))
FROM numbers(12)
ORDER BY number;

-- The original crashing query from the CI report (STID 3702-5bce)
SELECT multiIf(
    (number % 2) = 0, toUInt32(1),
    (number % 3) = 0, toUInt32(2),
    toUInt8(3))
FROM numbers(10)
ORDER BY number;

-- multiIf with mixed float return types: Float32 -> Float64
-- Exercises the float-to-float nativeCast path (CreateFPCast)
SELECT multiIf(
    (number % 2) = 0, toFloat64(1.5),
    (number % 3) = 0, toFloat32(2.5),
    toFloat32(3.5))
FROM numbers(6)
ORDER BY number;

-- Mixed integer widths in arithmetic expressions (also uses nativeCast)
SELECT toUInt8(number) + toUInt32(100)
FROM numbers(5)
ORDER BY number;

-- Wider spread of integer types: UInt8, UInt16, UInt32, UInt64
SELECT multiIf(
    (number % 4) = 0, toUInt64(100),
    (number % 4) = 1, toUInt32(200),
    (number % 4) = 2, toUInt16(300),
    toUInt8(255))
FROM numbers(8)
ORDER BY number;
