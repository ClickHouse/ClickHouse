-- Tags: memory-engine

-- A rewrite to a subcolumn must land on the subcolumn it means. Subcolumn names are flat, so a
-- Tuple element or a JSON path can claim the name of an automatic subcolumn with the same type,
-- and an enclosing Nullable can wrap the automatic subcolumn. Every query below must give the same
-- answer with the optimization on and off.

SET enable_analyzer = 1;

-- A Tuple element named `<sibling>.<automatic name>` claims the sibling's automatic subcolumn.
-- Memory, because the Map cases collide on file names in MergeTree.

DROP TABLE IF EXISTS t_shadowed_string_size;
CREATE TABLE t_shadowed_string_size (c Tuple(`a.size` UInt64, `a` String)) ENGINE = Memory;
INSERT INTO t_shadowed_string_size VALUES ((99, 'abc')), ((0, ''));

SELECT 'string size';
SELECT length(c.a), empty(c.a), notEmpty(c.a) FROM t_shadowed_string_size SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(c.a), empty(c.a), notEmpty(c.a) FROM t_shadowed_string_size SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_array_size;
CREATE TABLE t_shadowed_array_size (c Tuple(`a.size0` UInt64, `a` Array(UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_array_size VALUES ((99, [10, 20])), ((7, []));

SELECT 'array size';
SELECT length(c.a), empty(c.a), notEmpty(c.a) FROM t_shadowed_array_size SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(c.a), empty(c.a), notEmpty(c.a) FROM t_shadowed_array_size SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_null_map;
CREATE TABLE t_shadowed_null_map (c Tuple(`a.null` UInt8, `a` Nullable(UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_null_map VALUES ((1, 5)), ((1, NULL));

SELECT 'null map';
SELECT isNull(c.a), isNotNull(c.a) FROM t_shadowed_null_map SETTINGS optimize_functions_to_subcolumns = 0;
SELECT isNull(c.a), isNotNull(c.a) FROM t_shadowed_null_map SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(c.a) FROM t_shadowed_null_map SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(c.a) FROM t_shadowed_null_map SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_map_keys;
CREATE TABLE t_shadowed_map_keys (c Tuple(`a.keys` Array(String), `a` Map(String, UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_map_keys VALUES ((['x', 'y'], {'k': 1}));

SELECT 'map keys';
SELECT mapKeys(c.a), mapContains(c.a, 'x') FROM t_shadowed_map_keys SETTINGS optimize_functions_to_subcolumns = 0;
SELECT mapKeys(c.a), mapContains(c.a, 'x') FROM t_shadowed_map_keys SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_map_values;
CREATE TABLE t_shadowed_map_values (c Tuple(`a.values` Array(UInt64), `a` Map(String, UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_map_values VALUES (([7, 8], {'k': 1}));

SELECT 'map values';
SELECT mapValues(c.a) FROM t_shadowed_map_values SETTINGS optimize_functions_to_subcolumns = 0;
SELECT mapValues(c.a) FROM t_shadowed_map_values SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_map_key;
CREATE TABLE t_shadowed_map_key (c Tuple(`a.key_foo` UInt64, `a` Map(String, UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_map_key VALUES ((99, {'foo': 1}));

SELECT 'map element';
SELECT c.a['foo'] FROM t_shadowed_map_key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT c.a['foo'] FROM t_shadowed_map_key SETTINGS optimize_functions_to_subcolumns = 1;

-- The automatic subcolumn is the one resolved, but an enclosing Nullable wraps it. `Array` and `Map`
-- cannot be inside `Nullable`, so `c.a` is exposed as a bare `Array` while `c.a.size0` is
-- `Nullable(UInt64)`, and `length` must give 0 for a NULL row, not NULL.

DROP TABLE IF EXISTS t_nullable_json;
CREATE TABLE t_nullable_json (c Nullable(JSON(`a` Array(Int64), `m` Map(String, Int64)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_json VALUES ('{"a":[1,2],"m":{"k":1}}'), (NULL);

SELECT 'nullable json';
SELECT length(c.a), empty(c.a), notEmpty(c.a), length(c.m) FROM t_nullable_json SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(c.a), empty(c.a), notEmpty(c.a), length(c.m) FROM t_nullable_json SETTINGS optimize_functions_to_subcolumns = 1;

SET enable_nullable_tuple_type = 1;
DROP TABLE IF EXISTS t_nullable_tuple;
CREATE TABLE t_nullable_tuple (c Nullable(Tuple(arr Array(Int64), m Map(String, Int64)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_tuple VALUES (([1, 2], {'k': 1})), (NULL);

SELECT 'nullable tuple';
SELECT length(c.arr), empty(c.arr), length(c.m), mapKeys(c.m) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(c.arr), empty(c.arr), length(c.m), mapKeys(c.m) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;

-- A JSON path can be named like an automatic subcolumn too. What `arr.size0` resolves to is decided
-- by the resolution order, but the rewrite must agree with the unoptimized query either way.

DROP TABLE IF EXISTS t_json_size_path;
CREATE TABLE t_json_size_path (arr Array(JSON), nested Array(Array(JSON)), m Map(String, JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_size_path VALUES ([('{"size0":7}'), ('{"size0":8}')], [[('{"size0":7}')]], {'k': '{"size0":9}'});

SELECT 'json size path';
SELECT length(arr), empty(arr), length(nested), length(m) FROM t_json_size_path SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(arr), empty(arr), length(nested), length(m) FROM t_json_size_path SETTINGS optimize_functions_to_subcolumns = 1;

-- A sibling element can flatten to the same name as a nested one, and declaration order decides
-- which one a read returns. Only the case where the sibling is declared first is wrong.

DROP TABLE IF EXISTS t_shadowed_tuple_element;
CREATE TABLE t_shadowed_tuple_element (c Tuple(`t.a` UInt64, t Tuple(a UInt64))) ENGINE = Memory;
INSERT INTO t_shadowed_tuple_element VALUES ((99, (1)));

SELECT 'tuple element';
SELECT tupleElement(c.t, 'a') FROM t_shadowed_tuple_element SETTINGS optimize_functions_to_subcolumns = 0;
SELECT tupleElement(c.t, 'a') FROM t_shadowed_tuple_element SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_nested_tuple_element;
CREATE TABLE t_nested_tuple_element (c Tuple(t Tuple(a UInt64), `t.a` UInt64)) ENGINE = Memory;
INSERT INTO t_nested_tuple_element VALUES (((1), 99));

SELECT 'tuple element, nested declared first';
SELECT tupleElement(c.t, 'a') FROM t_nested_tuple_element SETTINGS optimize_functions_to_subcolumns = 0;
SELECT tupleElement(c.t, 'a') FROM t_nested_tuple_element SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_variant_element;
CREATE TABLE t_shadowed_variant_element (c Tuple(`t.Int64` UInt64, t Variant(Int64, String))) ENGINE = Memory;
INSERT INTO t_shadowed_variant_element VALUES ((99, 7::Int64));

SELECT 'variant element';
SELECT variantElement(c.t, 'Int64') FROM t_shadowed_variant_element SETTINGS optimize_functions_to_subcolumns = 0;
SELECT variantElement(c.t, 'Int64') FROM t_shadowed_variant_element SETTINGS optimize_functions_to_subcolumns = 1;

-- Another column, or another element of the same column, can answer to the same flat name:
-- `<column>.<suffix>` is served by the shortest prefix that can answer it, so a `JSON j` with a path
-- `m` answers `j.m.key_k` while a physical `j.m` answers `j.m`. Only the substreams path shows which
-- one a rewrite would read.

DROP TABLE IF EXISTS t_shadowed_by_json_path;
CREATE TABLE t_shadowed_by_json_path
(j JSON(m Map(String, String), s String, n Nullable(String)), `j.m` Map(String, String), `j.s` String, `j.n` Nullable(String))
ENGINE = Memory;
INSERT INTO t_shadowed_by_json_path SELECT
    CAST('{"m":{"nokey":"json"},"s":"JSONSTRING","n":"jsonnn"}', 'JSON(m Map(String, String), s String, n Nullable(String))'),
    map('abc', 'physical', 'def', 'physical2'), '', NULL;

SELECT 'physical column shadowed by a json path';
SELECT `j.m`['nokey'], mapKeys(`j.m`), mapValues(`j.m`), mapContains(`j.m`, 'nokey'), length(`j.m`),
       length(`j.s`), empty(`j.s`), isNull(`j.n`)
FROM t_shadowed_by_json_path SETTINGS optimize_functions_to_subcolumns = 0;
SELECT `j.m`['nokey'], mapKeys(`j.m`), mapValues(`j.m`), mapContains(`j.m`, 'nokey'), length(`j.m`),
       length(`j.s`), empty(`j.s`), isNull(`j.n`)
FROM t_shadowed_by_json_path SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_element_shadowed_by_sibling_path;
CREATE TABLE t_element_shadowed_by_sibling_path (c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String)))) ENGINE = Memory;
INSERT INTO t_element_shadowed_by_sibling_path SELECT tuple(map('abc', 'physical'), CAST('{"m":{"nokey":"json"}}', 'JSON(m Map(String, String))'));

SELECT 'tuple element shadowed by a sibling json path';
SELECT c.`j.m`['nokey'] FROM t_element_shadowed_by_sibling_path SETTINGS optimize_functions_to_subcolumns = 0;
SELECT c.`j.m`['nokey'] FROM t_element_shadowed_by_sibling_path SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_distinct_paths;
CREATE TABLE t_shadowed_distinct_paths (j JSON(x JSON), `j.x` JSON) ENGINE = Memory;
INSERT INTO t_shadowed_distinct_paths SELECT CAST('{"x":{"fromjson":1}}', 'JSON(x JSON)'), CAST('{"fromphysical":2}', 'JSON');

DROP TABLE IF EXISTS t_distinct_paths_named_path;
CREATE TABLE t_distinct_paths_named_path
(j JSON(`__special_subcolumn_name_for_distinct_paths_calculation` Array(String), real Int64)) ENGINE = Memory;
INSERT INTO t_distinct_paths_named_path SELECT
    CAST('{"__special_subcolumn_name_for_distinct_paths_calculation":["BOGUS"],"real":1}',
         'JSON(`__special_subcolumn_name_for_distinct_paths_calculation` Array(String), real Int64)');

SELECT 'distinct json paths';
SELECT distinctJSONPaths(`j.x`) FROM t_shadowed_distinct_paths SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(`j.x`) FROM t_shadowed_distinct_paths SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_distinct_paths_named_path SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j) FROM t_distinct_paths_named_path SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadowed_array_of_json;
CREATE TABLE t_shadowed_array_of_json (j JSON, `j.arr` Dynamic) ENGINE = Memory;
INSERT INTO t_shadowed_array_of_json SELECT
    CAST('{"arr":[{"b":"FROM_JSON"}]}', 'JSON'), CAST([CAST('{"b":"FROM_PHYSICAL"}', 'JSON')], 'Dynamic');

DROP TABLE IF EXISTS t_array_of_json_shadowed_by_sibling;
CREATE TABLE t_array_of_json_shadowed_by_sibling (c Tuple(`j.arr` Dynamic, j JSON)) ENGINE = Memory;
INSERT INTO t_array_of_json_shadowed_by_sibling SELECT
    tuple(CAST([CAST('{"b":"FROM_PHYSICAL"}', 'JSON')], 'Dynamic'), CAST('{"arr":[{"b":"FROM_JSON"}]}', 'JSON'));

SELECT 'array of json element';
SELECT `j.arr`[1].b FROM t_shadowed_array_of_json SETTINGS optimize_functions_to_subcolumns = 0;
SELECT `j.arr`[1].b FROM t_shadowed_array_of_json SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c.`j.arr`[1].b FROM t_array_of_json_shadowed_by_sibling SETTINGS optimize_functions_to_subcolumns = 0;
SELECT c.`j.arr`[1].b FROM t_array_of_json_shadowed_by_sibling SETTINGS optimize_functions_to_subcolumns = 1;

-- The guard must not reject the ordinary case: every rewrite still fires when nothing claims the name.
-- The setting is randomized in CI, and these queries read it from the session.

DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_plain (s String, arr Array(UInt64), m Map(String, UInt64), n Nullable(UInt64), t Tuple(x UInt64), v Variant(Int64, String))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_plain VALUES ('abc', [1, 2], {'k': 1}, 5, (1), 7::Int64);

SET optimize_functions_to_subcolumns = 1;

SELECT 'still optimized';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM t_plain) WHERE explain LIKE '%s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT empty(s) FROM t_plain) WHERE explain LIKE '%s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT notEmpty(s) FROM t_plain) WHERE explain LIKE '%s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(arr) FROM t_plain) WHERE explain LIKE '%arr.size0%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT empty(arr) FROM t_plain) WHERE explain LIKE '%arr.size0%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT notEmpty(arr) FROM t_plain) WHERE explain LIKE '%arr.size0%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(m) FROM t_plain) WHERE explain LIKE '%m.size0%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT mapKeys(m) FROM t_plain) WHERE explain LIKE '%m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT mapValues(m) FROM t_plain) WHERE explain LIKE '%m.values%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT mapContains(m, 'k') FROM t_plain) WHERE explain LIKE '%m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT m['k'] FROM t_plain) WHERE explain LIKE '%m.key_k%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT isNull(n) FROM t_plain) WHERE explain LIKE '%n.null%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT isNotNull(n) FROM t_plain) WHERE explain LIKE '%n.null%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count(n) FROM t_plain) WHERE explain LIKE '%n.null%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(t, 'x') FROM t_plain) WHERE explain LIKE '%t.x%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT variantElement(v, 'Int64') FROM t_plain) WHERE explain LIKE '%v.Int64%';

-- A rewrite under a legitimate JSON path, and under a Tuple element, still fires: the source's
-- substreams path is a prefix of the rewritten subcolumn's. In the unshadowed case both settings
-- return the same value, so only the query tree can see whether the rewrite was lost.

DROP TABLE IF EXISTS t_json_path_plain;
CREATE TABLE t_json_path_plain (j JSON(m Map(String, String), s String)) ENGINE = Memory;
INSERT INTO t_json_path_plain SELECT CAST('{"m":{"k":"v"},"s":"abc"}', 'JSON(m Map(String, String), s String)');
SELECT count() FROM (EXPLAIN QUERY TREE SELECT j.m['k'] FROM t_json_path_plain) WHERE explain LIKE '%j.m.key_k%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT mapKeys(j.m) FROM t_json_path_plain) WHERE explain LIKE '%j.m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(j.s) FROM t_json_path_plain) WHERE explain LIKE '%j.s.size%';

DROP TABLE IF EXISTS t_json_array_plain;
CREATE TABLE t_json_array_plain (j JSON) ENGINE = Memory;
INSERT INTO t_json_array_plain SELECT CAST('{"arr":[{"b":"FROM_JSON"}]}', 'JSON');
SELECT count() FROM (EXPLAIN QUERY TREE SELECT j.arr[1].b FROM t_json_array_plain) WHERE explain LIKE '%Array(JSON)%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT distinctJSONPaths(j) FROM t_json_array_plain) WHERE explain LIKE '%groupArrayDistinct%';

DROP TABLE IF EXISTS t_nested_plain;
CREATE TABLE t_nested_plain (n Nested(a Int64, b String)) ENGINE = Memory;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(`n.a`) FROM t_nested_plain) WHERE explain LIKE '%n.a.size0%';

DROP TABLE IF EXISTS t_tuple_path_plain;
CREATE TABLE t_tuple_path_plain (c Tuple(m Map(String, String), arr Array(Int64))) ENGINE = Memory;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT c.m['k'] FROM t_tuple_path_plain) WHERE explain LIKE '%c.m.key_k%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(c.arr) FROM t_tuple_path_plain) WHERE explain LIKE '%c.arr.size0%';

DROP TABLE t_shadowed_string_size;
DROP TABLE t_shadowed_array_size;
DROP TABLE t_shadowed_null_map;
DROP TABLE t_shadowed_map_keys;
DROP TABLE t_shadowed_map_values;
DROP TABLE t_shadowed_map_key;
DROP TABLE t_nullable_json;
DROP TABLE t_nullable_tuple;
DROP TABLE t_json_size_path;
DROP TABLE t_shadowed_tuple_element;
DROP TABLE t_nested_tuple_element;
DROP TABLE t_shadowed_variant_element;
DROP TABLE t_plain;
DROP TABLE t_shadowed_by_json_path;
DROP TABLE t_element_shadowed_by_sibling_path;
DROP TABLE t_shadowed_distinct_paths;
DROP TABLE t_distinct_paths_named_path;
DROP TABLE t_shadowed_array_of_json;
DROP TABLE t_array_of_json_shadowed_by_sibling;
DROP TABLE t_json_path_plain;
DROP TABLE t_json_array_plain;
DROP TABLE t_nested_plain;
DROP TABLE t_tuple_path_plain;
