-- Each Map nested inside Array/Tuple has its own key set. Maps inside Variant, Dynamic,
-- JSON and Map keep the basic layout.

SET allow_experimental_map_key_columns = 1;

DROP TABLE IF EXISTS t_per_map;
CREATE TABLE t_per_map (id UInt64, t Tuple(a Map(String, UInt64), b Map(String, UInt64)), at Array(Tuple(c Map(String, UInt64), d Map(String, UInt64))))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_per_map VALUES (1, ({'a1': 1}, {'b1': 2}), [({'c1': 3}, {'d1': 4})]), (2, ({}, {'b2': 5}), []);
INSERT INTO t_per_map VALUES (3, ({'a2': 6}, {}), [({}, {'d2': 7}), ({'c2': 8}, {})]);
OPTIMIZE TABLE t_per_map FINAL;
SELECT * FROM t_per_map ORDER BY id;
SELECT t.a['a1'], t.b['b2'], at.c, at.d FROM t_per_map ORDER BY id;
SELECT column, substreams FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_per_map' AND active AND column IN ('t', 'at') ORDER BY column;
DROP TABLE t_per_map;

DROP TABLE IF EXISTS t_per_map_compact;
CREATE TABLE t_per_map_compact (id UInt64, t Tuple(a Map(String, UInt64), b Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = '10G', index_granularity = 1;
INSERT INTO t_per_map_compact VALUES (1, ({'a1': 1}, {'b1': 2})), (2, ({}, {'b2': 5}));
INSERT INTO t_per_map_compact VALUES (3, ({'a2': 6}, {}));
OPTIMIZE TABLE t_per_map_compact FINAL;
SELECT * FROM t_per_map_compact ORDER BY id;
SELECT column, substreams FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_per_map_compact' AND active AND column IN ('t', 'at') ORDER BY column;
DROP TABLE t_per_map_compact;

DROP TABLE IF EXISTS t_basic_maps;
CREATE TABLE t_basic_maps (id UInt64, v Variant(Map(String, UInt64), UInt8), d Dynamic, j JSON(p Map(String, UInt64)), mm Map(String, Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_basic_maps VALUES (1, map('a', 1), map('a', 1)::Map(String, UInt64), '{"p":{"a":1}}', {'x': {'y': 1}}), (2, 3::UInt8, 'x', '{}', {'z': {}});
INSERT INTO t_basic_maps VALUES (3, map('b', 2), map('b', 2)::Map(String, UInt64), '{"p":{"b":2}}', {});
OPTIMIZE TABLE t_basic_maps FINAL;
SELECT * FROM t_basic_maps ORDER BY id;
SELECT v.`Map(String, UInt64)`['b'], j.p['a'], mm['x']['y'] FROM t_basic_maps ORDER BY id;
-- Only the top-level Map has per-key streams.
SELECT column, arrayFilter(s -> s LIKE '%.exists.%' OR s LIKE '%.keys', substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_basic_maps' AND active AND column IN ('v', 'mm') ORDER BY column;
DROP TABLE t_basic_maps;
