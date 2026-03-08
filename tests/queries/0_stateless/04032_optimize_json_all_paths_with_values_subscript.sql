-- Test optimization: JSONAllPathsWithValues(json)['key'] and cast variant -> getSubcolumn for pushdown.
-- Requires both experimental settings.

SET allow_experimental_json_all_paths_with_values = 1;
SET allow_experimental_optimize_json_cast_to_map_access = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_04033_json_subscript;

CREATE TABLE t_04033_json_subscript
(
    id UInt64,
    json JSON
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_04033_json_subscript VALUES
    (1, '{"a": "x", "b": 42, "n": 100}'),
    (2, '{"a": "y", "b": 10}'),
    (3, '{"a": "x"}'),
    (4, '{}');

-- Equivalence: direct path vs JSONAllPathsWithValues subscript (optimized to getSubcolumn)
SELECT '-- direct vs via JSONAllPathsWithValues subscript';
SELECT id, json.a AS direct, JSONAllPathsWithValues(json)['a'] AS via_map
FROM t_04033_json_subscript
ORDER BY id;

-- WHERE with cast variant: same rows as native path
SELECT '-- WHERE JSONAllPathsWithValues(json)::Map(String,String)[''a''] = ''x''';
SELECT id, json FROM t_04033_json_subscript
WHERE JSONAllPathsWithValues(json)::Map(String, String)['a'] = 'x'
ORDER BY id;

SELECT '-- WHERE json.a = ''x'' (native path, same result)';
SELECT id, json FROM t_04033_json_subscript
WHERE json.a = 'x'
ORDER BY id;

-- Single-element extraction with cast
SELECT '-- SELECT subscript and cast';
SELECT id, JSONAllPathsWithValues(json)::Map(String, Int64)['b'] AS b_val
FROM t_04033_json_subscript
ORDER BY id;

-- Non-constant key: no optimization, query still correct (filter to one row so key is effectively constant)
SELECT '-- Non-constant key (no rewrite, correct result)';
SELECT id, JSONAllPathsWithValues(json)['a'] AS v
FROM t_04033_json_subscript
WHERE id = 1;

-- Table with ALIAS column using JSONAllPathsWithValues (expression is valid at CREATE time)
DROP TABLE IF EXISTS t_04033_json_alias;

CREATE TABLE t_04033_json_alias
(
    id UInt64,
    json JSON,
    as_map Map(String, String) ALIAS JSONAllPathsWithValues(json)::Map(String, String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_04033_json_alias VALUES
    (1, '{"a": "x", "b": "10"}'),
    (2, '{"a": "y"}'),
    (3, '{}');

SELECT '-- ALIAS column: WHERE as_map[''a''] = ''x''';
SELECT id, json FROM t_04033_json_alias
WHERE as_map['a'] = 'x'
ORDER BY id;

SELECT '-- ALIAS column: SELECT as_map[''a''], as_map[''b'']';
SELECT id, as_map['a'] AS a_val, as_map['b'] AS b_val
FROM t_04033_json_alias
ORDER BY id;

-- Verify optimization is applied: plan must contain getSubcolumn (direct and ALIAS)
SELECT '-- Optimization in plan (direct JSONAllPathsWithValues subscript)';
SELECT count() FROM (EXPLAIN PLAN actions=1 SELECT id FROM t_04033_json_subscript WHERE JSONAllPathsWithValues(json)['a'] = 'x') WHERE explain LIKE '%getSubcolumn%';

SELECT '-- Optimization in plan (ALIAS column subscript)';
SELECT count() FROM (EXPLAIN PLAN actions=1 SELECT id FROM t_04033_json_alias WHERE as_map['a'] = 'x') WHERE explain LIKE '%getSubcolumn%';

DROP TABLE t_04033_json_alias;
DROP TABLE t_04033_json_subscript;
