-- `optimize_inverse_dictionary_lookup` folds a predicate matching a single dictionary key into
-- `key_expr = key`. Floating-point values inside container keys (`Array`, `Map`, nested `Tuple`) are
-- compared numerically by equality (`[0.0] = [-0.0]` is true), while a dictionary lookup matches them
-- by representation. The results must be the same with the optimization on and off.

SET enable_analyzer = 1;

DROP DICTIONARY IF EXISTS dict_array;
DROP DICTIONARY IF EXISTS dict_array_composite;
DROP DICTIONARY IF EXISTS dict_map;
DROP TABLE IF EXISTS array_source;
DROP TABLE IF EXISTS array_composite_source;
DROP TABLE IF EXISTS map_source;
DROP TABLE IF EXISTS container_probes;

CREATE TABLE array_source (k Array(Float64), attr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO array_source VALUES ([nan], 'nan'), ([-0.0], 'negative zero'), ([1.5], 'one and a half');

CREATE DICTIONARY dict_array (k Array(Float64), attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'array_source')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE array_composite_source (k1 Array(Float64), k2 String, attr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO array_composite_source VALUES ([nan], 'a', 'nan'), ([-0.0], 'a', 'negative zero'), ([1.5], 'a', 'one and a half');

CREATE DICTIONARY dict_array_composite (k1 Array(Float64), k2 String, attr String DEFAULT '')
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'array_composite_source')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE map_source (k Map(String, Float64), attr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO map_source VALUES ({'x': nan}, 'nan'), ({'x': -0.0}, 'negative zero'), ({'x': 1.5}, 'one and a half');

CREATE DICTIONARY dict_map (k Map(String, Float64), attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'map_source')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE container_probes (id UInt8, a Array(Float64), m Map(String, Float64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO container_probes VALUES (1, [nan], {'x': nan}), (2, [0.0], {'x': 0.0}), (3, [-0.0], {'x': -0.0}), (4, [1.5], {'x': 1.5}), (5, [2], {'x': 2});

SET optimize_inverse_dictionary_lookup = 1;
SELECT 'optimization on';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', tuple(a)) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'one and a half';
SELECT groupArray(id) FROM container_probes WHERE NOT (dictGet('dict_array', 'attr', a) = 'negative zero');
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array_composite', 'attr', (a, 'a')) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array_composite', 'attr', (a, 'a')) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_map', 'attr', m) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_map', 'attr', m) = 'negative zero';

SET optimize_inverse_dictionary_lookup = 0;
SELECT 'optimization off';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', tuple(a)) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array', 'attr', a) = 'one and a half';
SELECT groupArray(id) FROM container_probes WHERE NOT (dictGet('dict_array', 'attr', a) = 'negative zero');
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array_composite', 'attr', (a, 'a')) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_array_composite', 'attr', (a, 'a')) = 'negative zero';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_map', 'attr', m) = 'nan';
SELECT groupArray(id) FROM container_probes WHERE dictGet('dict_map', 'attr', m) = 'negative zero';

DROP DICTIONARY dict_array;
DROP DICTIONARY dict_array_composite;
DROP DICTIONARY dict_map;
DROP TABLE array_source;
DROP TABLE array_composite_source;
DROP TABLE map_source;
DROP TABLE container_probes;
