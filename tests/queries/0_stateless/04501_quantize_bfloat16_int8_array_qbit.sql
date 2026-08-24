-- quantizeBFloat16ToInt8 / dequantizeInt8ToBFloat16 applied to whole vectors (Array and QBit),
-- in addition to the scalar overloads. The vector overloads apply the same 256-level Gaussian
-- Lloyd-Max codec elementwise, so they must agree with the scalar version applied via arrayMap.

-- Array overloads and their result types.
SELECT quantizeBFloat16ToInt8([0.1, -0.5, 2.0, -3.25]::Array(BFloat16)) AS codes;
SELECT arrayMap(x -> round(toFloat32(x), 4), dequantizeInt8ToBFloat16([10, -49, 116, -126]::Array(Int8))) AS reconstructed;
SELECT toTypeName(quantizeBFloat16ToInt8([0.1]::Array(BFloat16))), toTypeName(dequantizeInt8ToBFloat16([1]::Array(Int8)));

-- The Array overload agrees with the scalar overload applied elementwise via arrayMap.
WITH arrayMap(i -> toBFloat16(0.05 * (i - 64)), range(128)) AS v
SELECT quantizeBFloat16ToInt8(v) = arrayMap(x -> quantizeBFloat16ToInt8(x), v) AS array_matches_scalar;

WITH arrayMap(i -> toInt8(i - 128), range(256)) AS codes
SELECT dequantizeInt8ToBFloat16(codes) = arrayMap(c -> dequantizeInt8ToBFloat16(c), codes) AS array_matches_scalar;

-- QBit overload: quantizing a QBit(BFloat16) agrees with quantizing the reconstructed Array.
WITH arrayMap(i -> toBFloat16(sin(i)), range(64)) AS v
SELECT
    quantizeBFloat16ToInt8(v) = CAST(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 64))) AS Array(Int8)) AS qbit_quantize_matches,
    toTypeName(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 64)))) AS qbit_quantize_type;

-- QBit overload: dequantizing a QBit(Int8) agrees with dequantizing the reconstructed Array.
WITH arrayMap(i -> toInt8(i - 128), range(64)) AS codes
SELECT
    dequantizeInt8ToBFloat16(codes) = CAST(dequantizeInt8ToBFloat16(CAST(codes AS QBit(Int8, 64))) AS Array(BFloat16)) AS qbit_dequantize_matches,
    toTypeName(dequantizeInt8ToBFloat16(CAST(codes AS QBit(Int8, 64)))) AS qbit_dequantize_type;

-- Strided QBit preserves the dimension and stride.
WITH arrayMap(i -> toBFloat16(cos(i)), range(16)) AS v
SELECT
    quantizeBFloat16ToInt8(v) = CAST(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 16, 8))) AS Array(Int8)) AS strided_matches,
    toTypeName(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 16, 8)))) AS strided_type;

-- A dimension that is not a multiple of 8 exercises the padding path.
WITH [0.1, -0.5, 2.0, -3.25, 0.7]::Array(BFloat16) AS v
SELECT quantizeBFloat16ToInt8(v) = CAST(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 5))) AS Array(Int8)) AS padding_matches;

-- Multiple rows.
DROP TABLE IF EXISTS test_04501;
CREATE TABLE test_04501 (v Array(BFloat16)) ENGINE = Memory;
INSERT INTO test_04501 VALUES ([0.1, -0.5, 2.0, -3.25]), ([1.0, 2.0, -1.0, 0.0]), ([-0.01, 0.02, -0.03, 0.04]);
SELECT quantizeBFloat16ToInt8(v) = CAST(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 4))) AS Array(Int8)) AS rows_match FROM test_04501 ORDER BY v;
DROP TABLE test_04501;

-- Nullable(QBit) keeps NULL rows NULL.
DROP TABLE IF EXISTS test_04501_nullable;
CREATE TABLE test_04501_nullable (v Nullable(String)) ENGINE = Memory;
INSERT INTO test_04501_nullable VALUES ('[0.1,-0.5,2.0,-3.25]'), (NULL), ('[1.0,2.0,-1.0,0.0]');
SELECT CAST(CAST(quantizeBFloat16ToInt8(CAST(v AS Nullable(QBit(BFloat16, 4)))) AS Nullable(QBit(Int8, 4))) AS Nullable(String)) AS q
FROM test_04501_nullable ORDER BY v;
DROP TABLE test_04501_nullable;

-- Round trip over a QBit reconstructs the same values as over an Array.
WITH arrayMap(i -> toBFloat16(0.05 * (i - 64)), range(128)) AS v
SELECT CAST(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(CAST(v AS QBit(BFloat16, 128)))) AS Array(BFloat16))
     = dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(v)) AS roundtrip_matches;

-- Type checks: unsupported element types are rejected.
SELECT quantizeBFloat16ToInt8([1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dequantizeInt8ToBFloat16([1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT quantizeBFloat16ToInt8(CAST([1, 2, 3, 4] AS QBit(Float32, 4))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dequantizeInt8ToBFloat16(CAST([1, 2, 3, 4] AS QBit(BFloat16, 4))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
