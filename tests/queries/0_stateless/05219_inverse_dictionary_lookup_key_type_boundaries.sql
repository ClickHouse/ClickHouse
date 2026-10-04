SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 0;

-- Numeric widening preserves equality, membership, and dictionary-subquery comparisons.
CREATE TABLE numeric_key_source (f32 Float32, f64 Float64, attr String) ENGINE = Memory;
INSERT INTO numeric_key_source VALUES (1, 1, 'single'), (2, 2, 'many'), (3, 3, 'many');
CREATE DICTIONARY numeric_key_float32 (f32 Float32, attr String DEFAULT '')
PRIMARY KEY f32 SOURCE(CLICKHOUSE(TABLE 'numeric_key_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY numeric_key_float64 (f64 Float64, attr String DEFAULT '')
PRIMARY KEY f64 SOURCE(CLICKHOUSE(TABLE 'numeric_key_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE numeric_key_probes (n UInt16) ENGINE = Memory;
INSERT INTO numeric_key_probes VALUES (1), (2), (4);

SELECT n,
    dictGet('numeric_key_float32', 'attr', tuple(n)) = 'single',
    dictGet('numeric_key_float32', 'attr', tuple(n)) = 'many',
    dictGet('numeric_key_float32', 'attr', tuple(n)) LIKE 'ma%',
    dictGet('numeric_key_float64', 'attr', toUInt32(n)) = 'single',
    dictGet('numeric_key_float64', 'attr', toFloat32(n)) = 'many',
    dictGet('numeric_key_float64', 'attr', toLowCardinality(toNullable(n))) LIKE 'ma%'
FROM numeric_key_probes ORDER BY n SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT n,
    dictGet('numeric_key_float32', 'attr', tuple(n)) = 'single',
    dictGet('numeric_key_float32', 'attr', tuple(n)) = 'many',
    dictGet('numeric_key_float32', 'attr', tuple(n)) LIKE 'ma%',
    dictGet('numeric_key_float64', 'attr', toUInt32(n)) = 'single',
    dictGet('numeric_key_float64', 'attr', toFloat32(n)) = 'many',
    dictGet('numeric_key_float64', 'attr', toLowCardinality(toNullable(n))) LIKE 'ma%'
FROM numeric_key_probes ORDER BY n SETTINGS optimize_inverse_dictionary_lookup = 1;

-- Integers exceeding the floating-point mantissa retain the dictionary's conversion errors.
CREATE TABLE numeric_key_precision (u32 UInt32, u64 UInt64) ENGINE = Memory;
INSERT INTO numeric_key_precision VALUES (16777217, 9007199254740993);
SELECT dictGet('numeric_key_float32', 'attr', u32) = 'single' FROM numeric_key_precision
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT dictGet('numeric_key_float32', 'attr', u32) = 'single' FROM numeric_key_precision
SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_CONVERT_TYPE }
SELECT dictGet('numeric_key_float64', 'attr', u64) = 'missing' FROM numeric_key_precision
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT dictGet('numeric_key_float64', 'attr', u64) = 'missing' FROM numeric_key_precision
SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_CONVERT_TYPE }

-- Nulls inside container keys are values and do not make the outer key comparison nullable.
CREATE TABLE container_key_source
(
    a Array(Nullable(UInt8)),
    m Map(String, Nullable(UInt8)),
    s String,
    attr String
) ENGINE = Memory;
INSERT INTO container_key_source VALUES ([NULL], {'x': NULL}, 'a', 'hit'), ([1], {'x': 1}, 'a', 'other');
CREATE DICTIONARY container_array (a Array(Nullable(UInt8)), attr String DEFAULT '')
PRIMARY KEY a SOURCE(CLICKHOUSE(TABLE 'container_key_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY container_map (m Map(String, Nullable(UInt8)), attr String DEFAULT '')
PRIMARY KEY m SOURCE(CLICKHOUSE(TABLE 'container_key_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY container_composite (a Array(Nullable(UInt8)), s String, attr String DEFAULT '')
PRIMARY KEY a, s SOURCE(CLICKHOUSE(TABLE 'container_key_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

SELECT a,
    dictGet('container_array', 'attr', a) = 'hit',
    dictGet('container_map', 'attr', m) = 'hit',
    dictGet('container_composite', 'attr', (a, s)) = 'hit'
FROM container_key_source ORDER BY attr SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT a,
    dictGet('container_array', 'attr', a) = 'hit',
    dictGet('container_map', 'attr', m) = 'hit',
    dictGet('container_composite', 'attr', (a, s)) = 'hit'
FROM container_key_source ORDER BY attr SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY container_composite;
DROP DICTIONARY container_map;
DROP DICTIONARY container_array;
DROP TABLE container_key_source;
DROP TABLE numeric_key_precision;
DROP TABLE numeric_key_probes;
DROP DICTIONARY numeric_key_float64;
DROP DICTIONARY numeric_key_float32;
DROP TABLE numeric_key_source;
