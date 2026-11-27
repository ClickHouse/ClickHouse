-- `dictGetKeys` must treat IEEE signed zero consistently with SQL `equals`: `-0.0` and `+0.0` are
-- equal, yet they have different bit patterns. The vector path hashes the attribute value as a
-- prefilter before confirming a candidate with `equals`, so without normalizing signed zero in that
-- hash a query value of `-0.0` could miss a dictionary row storing `+0.0` (or vice versa) on the
-- vector path, while the constant path (which compares with `equals` directly) matches it. This test
-- pins the two paths together for `Float32` and `Float64` and mixes both zero signs in the dictionary.

DROP DICTIONARY IF EXISTS dict_signed_zero;
DROP TABLE IF EXISTS src_signed_zero;

CREATE TABLE src_signed_zero
(
    id  UInt64,
    f32 Float32,
    f64 Float64
)
ENGINE = Memory;

-- Keys 1 and 3 store +0.0, keys 4 and 5 store -0.0 (built with `-1 * 0`), key 2 stores a non-zero value.
INSERT INTO src_signed_zero VALUES (1, 0.0, 0.0), (2, 1.5, 2.5), (3, 0.0, 0.0);
INSERT INTO src_signed_zero SELECT 4, toFloat32(-1) * toFloat32(0), -1.0 * toFloat64(0);
INSERT INTO src_signed_zero SELECT 5, toFloat32(-1) * toFloat32(0), -1.0 * toFloat64(0);

CREATE DICTIONARY dict_signed_zero
(
    id  UInt64,
    f32 Float32,
    f64 Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_signed_zero'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Sanity: the test really uses negative zero (both must be 1)';
SELECT
    reinterpretAsUInt64(-1.0 * toFloat64(0)) = 0x8000000000000000 AS f64_is_negative_zero,
    reinterpretAsUInt32(toFloat32(toFloat32(-1) * toFloat32(0))) = 0x80000000 AS f32_is_negative_zero;

SELECT 'Float64: query -0.0 matches both stored +0.0 and -0.0 (constant then vector)';
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', -1.0 * toFloat64(0)));
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', materialize(-1.0 * toFloat64(0))));

SELECT 'Float64: query +0.0 matches both stored +0.0 and -0.0 (constant then vector)';
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', toFloat64(0)));
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', materialize(toFloat64(0))));

SELECT 'Float32: query -0.0 matches both stored +0.0 and -0.0 (constant then vector)';
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f32', toFloat32(-1) * toFloat32(0)));
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f32', materialize(toFloat32(-1) * toFloat32(0))));

SELECT 'Non-zero value still matches its single key (constant then vector)';
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', toFloat64(2.5)));
SELECT arraySort(dictGetKeys('dict_signed_zero', 'f64', materialize(toFloat64(2.5))));

SELECT 'Constant path equals vector path for +0.0, -0.0 and a non-zero value (must all be 1)';
SELECT
    dictGetKeys('dict_signed_zero', 'f64', z)  = dictGetKeys('dict_signed_zero', 'f64', materialize(z))  AS f64_consistent,
    dictGetKeys('dict_signed_zero', 'f32', zf) = dictGetKeys('dict_signed_zero', 'f32', materialize(zf)) AS f32_consistent
FROM
(
    SELECT
        arrayJoin([toFloat64(0), -1.0 * toFloat64(0), toFloat64(2.5)]) AS z,
        arrayJoin([toFloat32(0), toFloat32(-1) * toFloat32(0), toFloat32(1.5)]) AS zf
);

DROP DICTIONARY dict_signed_zero;
DROP TABLE src_signed_zero;
