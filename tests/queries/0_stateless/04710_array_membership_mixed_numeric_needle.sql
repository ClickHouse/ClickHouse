-- Array membership must use the same value comparison as the scalar `=` operator, so a needle is
-- never matched against an element it is not equal to. Every row prints the function result next to
-- an independent oracle built from `=`; the two must agree.

-- Not rewritten into `has`, otherwise the oracle would be the function under test.
SET optimize_rewrite_array_exists_to_has = 0;
SET allow_suspicious_low_cardinality_types = 1;

-- The arrays must be materialized: a constant array literal is answered by a different, already
-- correct code path, so a literal-only test passes even on unfixed code.
CREATE TABLE i32 (v Array(Int32)) ENGINE = Memory;
INSERT INTO i32 VALUES ([1, -1]);
CREATE TABLE i64 (v Array(Int64)) ENGINE = Memory;
INSERT INTO i64 VALUES ([1, -1]);
CREATE TABLE u32 (v Array(UInt32)) ENGINE = Memory;
INSERT INTO u32 VALUES ([4294967295, 7]);
CREATE TABLE u64 (v Array(UInt64)) ENGINE = Memory;
INSERT INTO u64 VALUES ([18446744073709551615, 1]);
CREATE TABLE i16 (v Array(Int16)) ENGINE = Memory;
INSERT INTO i16 VALUES ([1, -1]);
CREATE TABLE i8 (v Array(Int8)) ENGINE = Memory;
INSERT INTO i8 VALUES ([1, -1]);
CREATE TABLE u8 (v Array(UInt8)) ENGINE = Memory;
INSERT INTO u8 VALUES ([255, 1]);

SELECT '-- signed element, same-width unsigned needle';
SELECT 'Int32/UInt32', has(v, 4294967295::UInt32) AS got, arrayExists(x -> x = 4294967295::UInt32, v) AS oracle FROM i32;
SELECT 'Int64/UInt64', has(v, 18446744073709551615::UInt64) AS got, arrayExists(x -> x = 18446744073709551615::UInt64, v) AS oracle FROM i64;

SELECT '-- unsigned element, same-width signed needle';
SELECT 'UInt32/Int32', has(v, -1::Int32) AS got, arrayExists(x -> x = -1::Int32, v) AS oracle FROM u32;
SELECT 'UInt64/Int64', has(v, -1::Int64) AS got, arrayExists(x -> x = -1::Int64, v) AS oracle FROM u64;

SELECT '-- narrow widths (integer promotion already made these correct)';
SELECT 'Int16/UInt16', has(v, 65535::UInt16) AS got, arrayExists(x -> x = 65535::UInt16, v) AS oracle FROM i16;
SELECT 'Int8/UInt8', has(v, 255::UInt8) AS got, arrayExists(x -> x = 255::UInt8, v) AS oracle FROM i8;
SELECT 'UInt8/Int8', has(v, -1::Int8) AS got, arrayExists(x -> x = -1::Int8, v) AS oracle FROM u8;

SELECT '-- representable mixed-sign needles must keep matching';
SELECT 'Int32/UInt32 rep', has(v, 1::UInt32) AS got, arrayExists(x -> x = 1::UInt32, v) AS oracle FROM i32;
SELECT 'Int64/UInt64 rep', has(v, 1::UInt64) AS got, arrayExists(x -> x = 1::UInt64, v) AS oracle FROM i64;
SELECT 'UInt32/Int32 rep', has(v, 7::Int32) AS got, arrayExists(x -> x = 7::Int32, v) AS oracle FROM u32;
SELECT 'UInt64/Int64 rep', has(v, 1::Int64) AS got, arrayExists(x -> x = 1::Int64, v) AS oracle FROM u64;

SELECT '-- sibling functions sharing the comparison';
SELECT 'indexOf', indexOf(v, 4294967295::UInt32) AS got, arrayFirstIndex(x -> x = 4294967295::UInt32, v) AS oracle FROM i32;
SELECT 'indexOf rep', indexOf(v, 1::UInt32) AS got, arrayFirstIndex(x -> x = 1::UInt32, v) AS oracle FROM i32;
SELECT 'countEqual', countEqual(v, 4294967295::UInt32) AS got, length(arrayFilter(x -> x = 4294967295::UInt32, v)) AS oracle FROM i32;
SELECT 'countEqual rep', countEqual(v, 1::UInt32) AS got, length(arrayFilter(x -> x = 1::UInt32, v)) AS oracle FROM i32;

SELECT '-- indexOfAssumeSorted: the binary search ordering must match the equality';
CREATE TABLE su64 (v Array(UInt64)) ENGINE = Memory;
INSERT INTO su64 VALUES ([1, 5, 18446744073709551615]);
CREATE TABLE si32 (v Array(Int32)) ENGINE = Memory;
INSERT INTO si32 VALUES ([-5, -1, 3, 7]);
SELECT 'sorted UInt64/Int64', indexOfAssumeSorted(v, -1::Int64) AS got, arrayFirstIndex(x -> x = -1::Int64, v) AS oracle FROM su64;
SELECT 'sorted UInt64/Int64 rep', indexOfAssumeSorted(v, 5::Int64) AS got, arrayFirstIndex(x -> x = 5::Int64, v) AS oracle FROM su64;
SELECT 'sorted Int32/UInt32', indexOfAssumeSorted(v, 4294967295::UInt32) AS got, arrayFirstIndex(x -> x = 4294967295::UInt32, v) AS oracle FROM si32;
SELECT 'sorted Int32/UInt32 rep', indexOfAssumeSorted(v, 3::UInt32) AS got, arrayFirstIndex(x -> x = 3::UInt32, v) AS oracle FROM si32;

SELECT '-- Map key/value lookups use the same helpers';
CREATE TABLE mk (m Map(Int64, String)) ENGINE = Memory;
INSERT INTO mk VALUES (map(1, 'a', -1, 'b'));
CREATE TABLE mv (m Map(String, Int64)) ENGINE = Memory;
INSERT INTO mv VALUES (map('a', 1, 'b', -1));
SELECT 'mapContains', mapContains(m, 18446744073709551615::UInt64) AS got, arrayExists(x -> x = 18446744073709551615::UInt64, mapKeys(m)) AS oracle FROM mk;
SELECT 'mapContains rep', mapContains(m, 1::UInt64) AS got, arrayExists(x -> x = 1::UInt64, mapKeys(m)) AS oracle FROM mk;
SELECT 'mapContainsValue', mapContainsValue(m, 18446744073709551615::UInt64) AS got, arrayExists(x -> x = 18446744073709551615::UInt64, mapValues(m)) AS oracle FROM mv;
SELECT 'mapContainsValue rep', mapContainsValue(m, 1::UInt64) AS got, arrayExists(x -> x = 1::UInt64, mapValues(m)) AS oracle FROM mv;

SELECT '-- non-constant needle';
CREATE TABLE nc (v Array(Int32), n UInt32) ENGINE = Memory;
INSERT INTO nc VALUES ([1, -1], 4294967295), ([1, -1], 1);
SELECT 'vector needle', n, has(v, n) AS got, arrayExists(x -> x = n, v) AS oracle FROM nc ORDER BY n;

SELECT '-- Nullable elements';
CREATE TABLE nn32 (v Array(Nullable(Int32))) ENGINE = Memory;
INSERT INTO nn32 VALUES ([1, NULL, -1]);
SELECT 'Nullable(Int32)', has(v, 4294967295::UInt32) AS got, arrayExists(x -> x = 4294967295::UInt32, v) AS oracle FROM nn32;
SELECT 'Nullable(Int32) rep', has(v, 1::UInt32) AS got, arrayExists(x -> x = 1::UInt32, v) AS oracle FROM nn32;
SELECT 'Nullable(Int32) NULL', has(v, NULL) AS got FROM nn32;

SELECT '-- LowCardinality elements: the dictionary shortcut must not derive an index from a wrapping cast';
CREATE TABLE lc32 (v Array(LowCardinality(Int32)), n UInt32) ENGINE = Memory;
INSERT INTO lc32 VALUES ([1, -1], 4294967295), ([1, -1], 1);
CREATE TABLE lc64 (v Array(LowCardinality(Int64))) ENGINE = Memory;
INSERT INTO lc64 VALUES ([1, -1]);
CREATE TABLE lcn (v Array(LowCardinality(Nullable(Int32)))) ENGINE = Memory;
INSERT INTO lcn VALUES ([1, NULL, -1]);
SELECT 'LC(Int32) const', has(v, 4294967295::UInt32) AS got, arrayExists(x -> x = 4294967295::UInt32, v) AS oracle FROM lc32 LIMIT 1;
SELECT 'LC(Int32) const rep', has(v, 1::UInt32) AS got, arrayExists(x -> x = 1::UInt32, v) AS oracle FROM lc32 LIMIT 1;
SELECT 'LC(Int64) const', has(v, 18446744073709551615::UInt64) AS got, arrayExists(x -> x = 18446744073709551615::UInt64, v) AS oracle FROM lc64;
SELECT 'LC(Int64) const rep', has(v, 1::UInt64) AS got, arrayExists(x -> x = 1::UInt64, v) AS oracle FROM lc64;
SELECT 'LC(Int32) vector', n, has(v, n) AS got, arrayExists(x -> x = n, v) AS oracle FROM lc32 ORDER BY n;
SELECT 'LC(Nullable(Int32)) const', has(v, 4294967295::UInt32) AS got, arrayExists(x -> x = 4294967295::UInt32, v) AS oracle FROM lcn;
SELECT 'LC(Nullable(Int32)) const rep', has(v, 1::UInt32) AS got, arrayExists(x -> x = 1::UInt32, v) AS oracle FROM lcn;
SELECT 'LC(Nullable(Int32)) NULL', has(v, NULL) AS got FROM lcn;
SELECT 'LC(Int32) indexOf', indexOf(v, 4294967295::UInt32) AS got, arrayFirstIndex(x -> x = 4294967295::UInt32, v) AS oracle FROM lc32 LIMIT 1;
SELECT 'LC(Int32) countEqual', countEqual(v, 4294967295::UInt32) AS got, length(arrayFilter(x -> x = 4294967295::UInt32, v)) AS oracle FROM lc32 LIMIT 1;

SELECT '-- integer/float mixed domain: the cast must not lose precision';
CREATE TABLE f64 (v Array(Float64)) ENGINE = Memory;
INSERT INTO f64 VALUES ([9007199254740993.0, 1.0]);
CREATE TABLE bi64 (v Array(Int64)) ENGINE = Memory;
INSERT INTO bi64 VALUES ([9007199254740993, 1]);
CREATE TABLE f32 (v Array(Float32)) ENGINE = Memory;
INSERT INTO f32 VALUES ([16777217.0, 1.0]);
SELECT 'Float64/Int64', has(v, 9007199254740993::Int64) AS got, arrayExists(x -> x = 9007199254740993::Int64, v) AS oracle FROM f64;
SELECT 'Float64/Int64 rep', has(v, 1::Int64) AS got, arrayExists(x -> x = 1::Int64, v) AS oracle FROM f64;
SELECT 'Int64/Float64', has(v, 9007199254740992.0::Float64) AS got, arrayExists(x -> x = 9007199254740992.0::Float64, v) AS oracle FROM bi64;
SELECT 'Int64/Float64 rep', has(v, 1.0::Float64) AS got, arrayExists(x -> x = 1.0::Float64, v) AS oracle FROM bi64;
SELECT 'Float32/Int64', has(v, 16777217::Int64) AS got, arrayExists(x -> x = 16777217::Int64, v) AS oracle FROM f32;
SELECT 'Float32/Int32', has(v, 16777217::Int32) AS got, arrayExists(x -> x = 16777217::Int32, v) AS oracle FROM f32;
SELECT 'Float32/Int64 rep', has(v, 1::Int64) AS got, arrayExists(x -> x = 1::Int64, v) AS oracle FROM f32;

SELECT '-- NaN never equals anything';
CREATE TABLE fnan (v Array(Float64)) ENGINE = Memory;
INSERT INTO fnan VALUES ([nan, 1.0]);
SELECT 'NaN needle', has(v, nan) AS got, arrayExists(x -> x = nan, v) AS oracle FROM fnan;
SELECT 'NaN element, int needle', has(v, 1::UInt64) AS got, arrayExists(x -> x = 1::UInt64, v) AS oracle FROM fnan;

SELECT '-- Enum elements are unaffected';
CREATE TABLE en (v Array(Enum8('a' = 1, 'b' = -1))) ENGINE = Memory;
INSERT INTO en VALUES (['a', 'b']);
SELECT 'Enum8/UInt8', has(v, 255::UInt8) AS got FROM en;
SELECT 'Enum8/UInt8 rep', has(v, 1::UInt8) AS got FROM en;
