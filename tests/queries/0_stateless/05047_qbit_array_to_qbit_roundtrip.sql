-- Converting an Array to a QBit and back must reproduce the vector exactly. The conversion
-- transposes a block of rows at a time and takes a vectorised path for every whole group of eight
-- dimensions, so cover each element type, dimensions with and without a partial trailing group,
-- strides, NULL rows, and enough rows to cross a block boundary and leave a partial last block.
-- Values are random so that most bit planes are non-zero.

DROP TABLE IF EXISTS qbit_roundtrip_f64;
DROP TABLE IF EXISTS qbit_roundtrip_f32;
DROP TABLE IF EXISTS qbit_roundtrip_bf16;
DROP TABLE IF EXISTS qbit_roundtrip_i8;

CREATE TABLE qbit_roundtrip_f64 (arr Array(Float64)) ENGINE = Memory;
CREATE TABLE qbit_roundtrip_f32 (arr Array(Float32)) ENGINE = Memory;
CREATE TABLE qbit_roundtrip_bf16 (arr Array(BFloat16)) ENGINE = Memory;
CREATE TABLE qbit_roundtrip_i8 (arr Array(Int8)) ENGINE = Memory;

-- 100 rows: three whole blocks of 32 and a partial one.
INSERT INTO qbit_roundtrip_f64 SELECT arrayMap(i -> randNormal(0, 1), range(100)) FROM numbers(100);
INSERT INTO qbit_roundtrip_f32 SELECT arrayMap(i -> toFloat32(randNormal(0, 1)), range(100)) FROM numbers(100);
INSERT INTO qbit_roundtrip_bf16 SELECT arrayMap(i -> toBFloat16(randNormal(0, 1)), range(100)) FROM numbers(100);
INSERT INTO qbit_roundtrip_i8 SELECT arrayMap(i -> toInt8(rand(i) % 256 - 128), range(100)) FROM numbers(100);

SELECT 'dimension 100 (partial trailing group)';
SELECT countIf(arr != CAST(CAST(arr AS QBit(Float64, 100)) AS Array(Float64))) FROM qbit_roundtrip_f64;
SELECT countIf(arr != CAST(CAST(arr AS QBit(Float32, 100)) AS Array(Float32))) FROM qbit_roundtrip_f32;
SELECT countIf(arr != CAST(CAST(arr AS QBit(BFloat16, 100)) AS Array(BFloat16))) FROM qbit_roundtrip_bf16;
SELECT countIf(arr != CAST(CAST(arr AS QBit(Int8, 100)) AS Array(Int8))) FROM qbit_roundtrip_i8;

SELECT 'dimension 64 (whole groups only)';
SELECT countIf(head != CAST(CAST(head AS QBit(Float64, 64)) AS Array(Float64))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_f64);
SELECT countIf(head != CAST(CAST(head AS QBit(Float32, 64)) AS Array(Float32))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_f32);
SELECT countIf(head != CAST(CAST(head AS QBit(BFloat16, 64)) AS Array(BFloat16))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_bf16);
SELECT countIf(head != CAST(CAST(head AS QBit(Int8, 64)) AS Array(Int8))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_i8);

SELECT 'dimension 5 (shorter than one group)';
SELECT countIf(head != CAST(CAST(head AS QBit(Float64, 5)) AS Array(Float64))) FROM (SELECT arraySlice(arr, 1, 5) AS head FROM qbit_roundtrip_f64);
SELECT countIf(head != CAST(CAST(head AS QBit(BFloat16, 5)) AS Array(BFloat16))) FROM (SELECT arraySlice(arr, 1, 5) AS head FROM qbit_roundtrip_bf16);

SELECT 'strided';
SELECT countIf(head != CAST(CAST(head AS QBit(Float64, 64, 16)) AS Array(Float64))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_f64);
SELECT countIf(head != CAST(CAST(head AS QBit(Float32, 64, 8)) AS Array(Float32))) FROM (SELECT arraySlice(arr, 1, 64) AS head FROM qbit_roundtrip_f32);
SELECT countIf(head != CAST(CAST(head AS QBit(BFloat16, 96, 32)) AS Array(BFloat16))) FROM (SELECT arraySlice(arr, 1, 96) AS head FROM qbit_roundtrip_bf16);
SELECT countIf(head != CAST(CAST(head AS QBit(Int8, 96, 24)) AS Array(Int8))) FROM (SELECT arraySlice(arr, 1, 96) AS head FROM qbit_roundtrip_i8);

SELECT 'NULL rows stay NULL and leave their neighbours alone';
SET allow_experimental_nullable_tuple_type = 1;
DROP TABLE IF EXISTS qbit_roundtrip_nullable;
CREATE TABLE qbit_roundtrip_nullable (v Array(Nullable(Tuple(Array(Float64), String)))) ENGINE = Memory;
INSERT INTO qbit_roundtrip_nullable
SELECT if(number % 3 = 0, [NULL], [(arrayMap(i -> randNormal(0, 1), range(100)), 'x')]::Array(Nullable(Tuple(Array(Float64), String))))
FROM numbers(100);
SELECT
    countIf(c[1] IS NULL) AS nulls,
    countIf((c[1] IS NOT NULL) AND (tupleElement(assumeNotNull(c[1]), 1) != tupleElement(assumeNotNull(v[1]), 1))) AS mismatches
FROM (
    SELECT v, CAST(CAST(v, 'Array(Nullable(Tuple(QBit(Float64, 100), String)))'), 'Array(Nullable(Tuple(Array(Float64), String)))') AS c
    FROM qbit_roundtrip_nullable
);
DROP TABLE qbit_roundtrip_nullable;

SELECT 'all-zero vectors';
SELECT countIf(zeros != CAST(CAST(zeros AS QBit(Float64, 100)) AS Array(Float64))) FROM (
    SELECT arrayMap(i -> toFloat64(0), range(100)) AS zeros FROM numbers(40)
);

DROP TABLE qbit_roundtrip_f64;
DROP TABLE qbit_roundtrip_f32;
DROP TABLE qbit_roundtrip_bf16;
DROP TABLE qbit_roundtrip_i8;
