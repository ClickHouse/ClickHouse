-- `optimize_inverse_dictionary_lookup` folds a predicate matching a single dictionary key into
-- `key_expr = key`. A dictionary lookup, like membership in a set, matches floating-point keys by their
-- representation, while equality compares them numerically: `NaN` equals nothing, and `0` equals `-0`.
-- The results must be the same with the optimization on and off.

SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 0;

DROP DICTIONARY IF EXISTS dict_float;
DROP DICTIONARY IF EXISTS dict_float_composite;
DROP TABLE IF EXISTS float_source;
DROP TABLE IF EXISTS float_composite_source;
DROP TABLE IF EXISTS float_probes;

CREATE TABLE float_source (k Float64, attr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO float_source VALUES (nan, 'nan'), (-0.0, 'negative zero'), (1.5, 'one and a half');

CREATE DICTIONARY dict_float (k Float64, attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'float_source')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE float_composite_source (k1 Float64, k2 String, attr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO float_composite_source VALUES (nan, 'a', 'nan'), (-0.0, 'a', 'negative zero'), (1.5, 'a', 'one and a half');

CREATE DICTIONARY dict_float_composite (k1 Float64, k2 String, attr String DEFAULT '')
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'float_composite_source')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE float_probes (id UInt8, f Float64, f32 Float32) ENGINE = MergeTree ORDER BY id;
INSERT INTO float_probes VALUES (1, nan, nan), (2, 0.0, 0.0), (3, -0.0, -0.0), (4, 1.5, 1.5), (5, 2, 2);

SET optimize_inverse_dictionary_lookup = 1;
SELECT 'optimization on';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f32) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', tuple(f)) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'one and a half';
SELECT groupArray(id) FROM float_probes WHERE NOT (dictGet('dict_float', 'attr', f) = 'negative zero');
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float_composite', 'attr', (f, 'a')) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float_composite', 'attr', (f, 'a')) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) LIKE 'n%';

SET optimize_inverse_dictionary_lookup = 0;
SELECT 'optimization off';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f32) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', tuple(f)) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) = 'one and a half';
SELECT groupArray(id) FROM float_probes WHERE NOT (dictGet('dict_float', 'attr', f) = 'negative zero');
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float_composite', 'attr', (f, 'a')) = 'nan';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float_composite', 'attr', (f, 'a')) = 'negative zero';
SELECT groupArray(id) FROM float_probes WHERE dictGet('dict_float', 'attr', f) LIKE 'n%';

DROP DICTIONARY dict_float;
DROP DICTIONARY dict_float_composite;
DROP TABLE float_source;
DROP TABLE float_composite_source;
DROP TABLE float_probes;
