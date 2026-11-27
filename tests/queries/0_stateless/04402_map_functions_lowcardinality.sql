-- Map functions that return a Map by selecting or reordering the original key-value pairs
-- must preserve the key and value types of the input Map, including LowCardinality.
-- Previously mapFilter, mapSort and mapConcat silently dropped LowCardinality, which corrupted
-- the metadata of a table created from such an expression.

SELECT '-- mapFilter preserves LowCardinality of key and value';
SELECT toTypeName(mapFilter((k, v) -> k != 'a', map('a'::LowCardinality(String), 'x', 'b'::LowCardinality(String), 'y')));
SELECT toTypeName(mapFilter((k, v) -> 1, map('a', 'x'::LowCardinality(String))));
SELECT mapFilter((k, v) -> k != 'a', map('a'::LowCardinality(String), 'x', 'b'::LowCardinality(String), 'y'));

SELECT '-- mapSort and variants preserve LowCardinality';
SELECT toTypeName(mapSort(map('b'::LowCardinality(String), 'y', 'a'::LowCardinality(String), 'x')));
SELECT mapSort(map('b'::LowCardinality(String), 'y', 'a'::LowCardinality(String), 'x'));
SELECT toTypeName(mapReverseSort(map('a'::LowCardinality(String), 'x', 'b'::LowCardinality(String), 'y')));
SELECT toTypeName(mapPartialSort((k, v) -> k, 1, map('b'::LowCardinality(String), 'y', 'a'::LowCardinality(String), 'x')));

SELECT '-- mapConcat preserves LowCardinality when all inputs agree, otherwise falls back';
SELECT toTypeName(mapConcat(map('a'::LowCardinality(String), 'x'), map('b'::LowCardinality(String), 'y')));
SELECT mapConcat(map('a'::LowCardinality(String), 'x'), map('b'::LowCardinality(String), 'y'));
SELECT toTypeName(mapConcat(map('a'::LowCardinality(String), 'x'), map('b', 'y')));

SELECT '-- mapApply follows the lambda (just like arrayMap)';
SELECT toTypeName(mapApply((k, v) -> (k, v), map('a'::LowCardinality(String), 'x')));
SELECT toTypeName(mapApply((k, v) -> (k::LowCardinality(String), v), map('a', 'x')));

SELECT '-- subcolumn/scalar functions keep their existing behavior';
SELECT toTypeName(mapKeys(map('a'::LowCardinality(String), 'x')));
SELECT toTypeName(mapValues(map('a', 'x'::LowCardinality(String))));
SELECT toTypeName(mapContains(map('a'::LowCardinality(String), 'x'), 'a'));
SELECT toTypeName(mapExists((k, v) -> 1, map('a'::LowCardinality(String), 'x')));

SELECT '-- plain Map (no LowCardinality) is unaffected';
SELECT toTypeName(mapFilter((k, v) -> 1, map('a', 'x')));

SELECT '-- issue #108439: functions returning a Map must rebuild a nested Map value type';
SELECT toTypeName(mapFilter((k, v) -> 1, map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y'))));
SELECT mapFilter((k, v) -> 1, map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y')));
SELECT toTypeName(mapSort((k, v) -> k, map('b'::LowCardinality(String), map('x'::LowCardinality(String), 'y'), 'a'::LowCardinality(String), map('z'::LowCardinality(String), 'w'))));
SELECT mapSort((k, v) -> k, map('b'::LowCardinality(String), map('x'::LowCardinality(String), 'y'), 'a'::LowCardinality(String), map('z'::LowCardinality(String), 'w')));
SELECT toTypeName(mapReverseSort(map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y'))));
SELECT toTypeName(mapPartialSort((k, v) -> k, 1, map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y'))));
SELECT toTypeName(mapConcat(map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y'))));
SELECT mapConcat(map('a'::LowCardinality(String), map('x'::LowCardinality(String), 'y')));

SELECT '-- nested Map inside Array / Tuple value';
SELECT toTypeName(mapFilter((k, v) -> 1, map('a'::LowCardinality(String), [map('x'::LowCardinality(String), 'y')])));
SELECT mapFilter((k, v) -> 1, map('a'::LowCardinality(String), [map('x'::LowCardinality(String), 'y')]));
SELECT toTypeName(mapFilter((k, v) -> 1, map('a'::LowCardinality(String), tuple(map('x'::LowCardinality(String), 'y')))));
SELECT mapFilter((k, v) -> 1, map('a'::LowCardinality(String), tuple(map('x'::LowCardinality(String), 'y'))));

SELECT '-- nested Map with LowCardinality only in the inner value';
SELECT toTypeName(mapFilter((k, v) -> 1, map('a', map('x', 'y'::LowCardinality(String)))));

SELECT '-- the reproducer from the issue: CREATE TABLE AS keeps the column type and passes CHECK';
DROP TABLE IF EXISTS t_map_lc_src;
DROP TABLE IF EXISTS t_map_lc_dst;

-- Pin Map serialization to 'basic' so randomized 'with_buckets' serialization does not
-- reorder Map keys by hash bucket. This test checks the column type and CHECK TABLE, not
-- the Map key order, so the stored order must stay the insertion order.
CREATE TABLE t_map_lc_src
(
    key String,
    attributes_map Map(LowCardinality(String), String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

INSERT INTO t_map_lc_src VALUES ('key_1', {'attr_1': 'value_1', 'attr_2': 'value_2', 'attr_3': 'value_3'});

CREATE TABLE t_map_lc_dst
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic'
AS SELECT key, mapFilter((k, v) -> k != 'attr_1', attributes_map) AS attributes_map FROM t_map_lc_src;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_map_lc_dst' ORDER BY name;
SELECT key, attributes_map FROM t_map_lc_dst;
CHECK TABLE t_map_lc_dst SETTINGS check_query_single_value_result = 1;

DROP TABLE t_map_lc_src;
DROP TABLE t_map_lc_dst;
