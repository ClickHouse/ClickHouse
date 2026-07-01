SELECT 'Identity: trimming zero bits does nothing';
SELECT floatBitTrim(1.234::Float64, 0) = 1.234::Float64;
SELECT floatBitTrim(1.234::Float32, 0) = 1.234::Float32;
SELECT floatBitTrim(1.5::BFloat16, 0) = 1.5::BFloat16;

SELECT 'Low bits of the bit representation are zero after trimming.';
SELECT bin(reinterpretAsUInt64(floatBitTrim(1.234567890123::Float64, 30)));
SELECT bin(reinterpretAsUInt32(floatBitTrim(1.234::Float32, 10)));
SELECT bitAnd(reinterpretAsUInt16(floatBitTrim(1.234::BFloat16, 4)), bitShiftLeft(1::UInt16, 4) - 1);

SELECT 'Sign and exponent are preserved (clamped to mantissa width).';
SELECT floatBitTrim(3.14::Float64, 100) = floatBitTrim(3.14::Float64, 52);
SELECT floatBitTrim(-3.14::Float64, 100) = floatBitTrim(-3.14::Float64, 52);
SELECT floatBitTrim(3.14::Float32, 100) = floatBitTrim(3.14::Float32, 23);
SELECT floatBitTrim(-3.14::Float32, 100) = floatBitTrim(-3.14::Float32, 23);
SELECT floatBitTrim(3.14::BFloat16, 100) = floatBitTrim(3.14::BFloat16, 7);
SELECT floatBitTrim(-3.14::BFloat16, 100) = floatBitTrim(-3.14::BFloat16, 7);

SELECT 'Result type matches input float type.';
SELECT toTypeName(floatBitTrim(1.0::Float64, 5));
SELECT toTypeName(floatBitTrim(1.0::Float32, 5));
SELECT toTypeName(floatBitTrim(1.0::BFloat16, 3));

SELECT 'Per-row n via a column.';
SELECT n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, n)))
FROM (SELECT arrayJoin([0, 1, 2, 8, 16, 30, 52, 100]) AS n)
ORDER BY n;

SELECT n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, n)))
FROM (SELECT arrayJoin([0, 1, 2, 8, 16, 20, 23, 100]) AS n)
ORDER BY n;

SELECT n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, n)))
FROM (SELECT arrayJoin([0, 1, 2, 4, 6, 7, 8, 100]) AS n)
ORDER BY n;

-- Errors.
SELECT floatBitTrim('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT floatBitTrim(1.0::Float64, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT floatBitTrim(1.0::Float64, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floatBitTrim(1.0::Float64, materialize(-1::Int64)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floatBitTrim(1.0::Float64, n) FROM (SELECT arrayJoin([0, -1]) AS n) FORMAT Null; -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'NaN passes through unchanged.';
SELECT isNaN(floatBitTrim(nan::Float64, 0));
SELECT isNaN(floatBitTrim(nan::Float32, 0));
SELECT bin(reinterpretAsUInt16(floatBitTrim(nan::BFloat16, 0)));

SELECT 'NaN must not collapse to Inf when trimmed.';
SELECT isNaN(floatBitTrim(reinterpretAsFloat64(reinterpretAsUInt64(nan::Float64) + 1), 52));
SELECT isNaN(floatBitTrim(reinterpretAsFloat32(reinterpretAsUInt32(nan::Float32) + 1), 23));
-- Document that the Float32 -> BFloat16 cast actually produced the NaN bit
SELECT hex(reinterpretAsUInt16(CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16)));
SELECT bin(reinterpretAsUInt16(floatBitTrim(
    CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16), 7
)));

SELECT 'Sign bit of NaN is preserved.';
SELECT reinterpretAsUInt64(floatBitTrim(-nan::Float64, 52)) = reinterpretAsUInt64(-nan::Float64);
SELECT reinterpretAsUInt32(floatBitTrim(-nan::Float32, 23)) = reinterpretAsUInt32(-nan::Float32);

SELECT 'Full payload is preserved.';
SELECT reinterpretAsUInt64(floatBitTrim(reinterpretAsFloat64(toUInt64(0x7FF8000000000001)), 52)) = 0x7FF8000000000001;
SELECT reinterpretAsUInt32(floatBitTrim(reinterpretAsFloat32(toUInt32(0x7FC00001)), 23)) = 0x7FC00001;
SELECT reinterpretAsUInt32(floatBitTrim(reinterpretAsFloat32(toUInt32(0x7F800001)), 23)) = 0x7F800001;
SELECT hex(reinterpretAsUInt16(CAST(reinterpretAsFloat32(toUInt32(0x7FC10000)) AS BFloat16)));
SELECT hex(reinterpretAsUInt16(floatBitTrim(
    CAST(reinterpretAsFloat32(toUInt32(0x7FC10000)) AS BFloat16), 7
)));

SELECT 'Inf stays Inf.';
SELECT isInfinite(floatBitTrim(inf::Float64, 52));
SELECT isInfinite(floatBitTrim(-inf::Float32, 23));
SELECT hex(reinterpretAsUInt16(floatBitTrim(inf::BFloat16, 7)));

SELECT 'materialize(): per-row path equals const path.';
SELECT floatBitTrim(1.234::Float64, materialize(20)) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(1.234::Float32, materialize(10)) = floatBitTrim(1.234::Float32, 10);
SELECT floatBitTrim(1.5::BFloat16, materialize(3)) = floatBitTrim(1.5::BFloat16, 3);

SELECT 'Const vs per-row parity across many rows.';
SELECT count() = sum(
    floatBitTrim(v, 20) = floatBitTrim(v, materialize(20)) OR
    (isNaN(floatBitTrim(v, 20)) AND isNaN(floatBitTrim(v, materialize(20))))
)
FROM (SELECT arrayJoin([1.234, -3.14, 0.0, -0.0, inf, -inf, nan]::Array(Float64)) AS v);

SELECT count() = sum(
    floatBitTrim(v, 10) = floatBitTrim(v, materialize(10)) OR
    (isNaN(floatBitTrim(v, 10)) AND isNaN(floatBitTrim(v, materialize(10))))
)
FROM (SELECT arrayJoin([1.234, -3.14, 0.0, -0.0, inf, -inf, nan]::Array(Float32)) AS v);

SELECT count() = sum(
    reinterpretAsUInt16(floatBitTrim(v, 3)) =
    reinterpretAsUInt16(floatBitTrim(v, materialize(3)))
)
FROM (
    SELECT arrayJoin([
        1.5::BFloat16, -3.25::BFloat16, 0.0::BFloat16, -0.0::BFloat16,
        inf::BFloat16, -inf::BFloat16, nan::BFloat16
    ]) AS v
);

SELECT 'NaN passthrough across many rows.';
SELECT sum(isNaN(floatBitTrim(materialize(nan::Float64), 30))) FROM numbers(100);
SELECT sum(isNaN(floatBitTrim(materialize(nan::Float32), 15))) FROM numbers(100);

SELECT 'Subnormals behavior.';
SELECT floatBitTrim(reinterpretAsFloat64(toUInt64(1)), 1) = 0.0;
SELECT floatBitTrim(reinterpretAsFloat64(toUInt64(3)), 1) = reinterpretAsFloat64(toUInt64(2));
SELECT floatBitTrim(reinterpretAsFloat32(toUInt32(1)), 1) = 0.0::Float32;

SELECT 'SIMD parity over many rows (Float64): finite, NaN, Inf, subnormal.';
SELECT countIf(
    reinterpretAsUInt64(floatBitTrim(v, 20)) != reinterpretAsUInt64(floatBitTrim(v, materialize(20))))
FROM (
    SELECT arrayJoin([
        nan::Float64,
        -nan::Float64,
        inf::Float64,
        -inf::Float64,
        reinterpretAsFloat64(toUInt64(0x7FF8000000000001)),
        reinterpretAsFloat64(toUInt64(1)),
        reinterpretAsFloat64(toUInt64(0x000FFFFFFFFFFFFF)),
        0.0::Float64,
        -0.0::Float64,
        1.234::Float64,
        -3.14159::Float64
    ]) AS v
    FROM numbers(100)
);

SELECT 'SIMD parity over many rows (Float32): finite, NaN, Inf, subnormal.';
SELECT countIf(
    reinterpretAsUInt32(floatBitTrim(v, 10)) != reinterpretAsUInt32(floatBitTrim(v, materialize(10))))
FROM (
    SELECT arrayJoin([
        nan::Float32,
        -nan::Float32,
        inf::Float32,
        -inf::Float32,
        reinterpretAsFloat32(toUInt32(0x7FC00001)),
        reinterpretAsFloat32(toUInt32(1)),
        reinterpretAsFloat32(toUInt32(0x007FFFFF)),
        0.0::Float32,
        -0.0::Float32,
        1.234::Float32,
        -3.14159::Float32
    ]) AS v
    FROM numbers(100)
);

SELECT 'SIMD parity over many rows (BFloat16): finite, NaN, Inf, subnormal.';
SELECT countIf(
    reinterpretAsUInt16(floatBitTrim(v, 3)) != reinterpretAsUInt16(floatBitTrim(v, materialize(3))))
FROM (
    SELECT arrayJoin([
        nan::BFloat16,
        -nan::BFloat16,
        inf::BFloat16,
        -inf::BFloat16,
        CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16),
        CAST(reinterpretAsFloat32(toUInt32(0x00010000)) AS BFloat16),
        CAST(reinterpretAsFloat32(toUInt32(0x007F0000)) AS BFloat16),
        0.0::BFloat16,
        -0.0::BFloat16,
        1.5::BFloat16,
        -3.25::BFloat16
    ]) AS v
    FROM numbers(100)
);
