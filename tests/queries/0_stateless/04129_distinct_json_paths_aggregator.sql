-- Exercise distinctJSONPaths / distinctJSONPathsAndTypes
-- (AggregateFunctions/AggregateFunctionDistinctJSONPaths.cpp) across typed paths,
-- shared dynamic paths, merging across partitions and empty/null inputs.

SELECT '--- single-row: flat object ---';
SELECT distinctJSONPaths(CAST('{"a":1,"b":"hi","c":true}', 'JSON'));
SELECT distinctJSONPathsAndTypes(CAST('{"a":1,"b":"hi","c":true}', 'JSON'));

SELECT '--- single-row: nested path gets dotted ---';
SELECT distinctJSONPaths(CAST('{"a":{"b":{"c":1}}}', 'JSON'));
SELECT distinctJSONPathsAndTypes(CAST('{"a":{"b":{"c":1}}}', 'JSON'));

SELECT '--- mixed paths across rows: aggregation adds all seen types ---';
DROP TABLE IF EXISTS distinct_json_paths_t;
CREATE TABLE distinct_json_paths_t (j JSON) ENGINE = Memory;
INSERT INTO distinct_json_paths_t VALUES
    ('{"a": 1}'),
    ('{"b": "x"}'),
    ('{"a": "hi"}'),
    ('{"c": [1, 2]}'),
    ('{"d": {"e": 1}}'),
    ('{}');
SELECT distinctJSONPaths(j) FROM distinct_json_paths_t;
SELECT distinctJSONPathsAndTypes(j) FROM distinct_json_paths_t;

SELECT '--- merge path via GROUP BY ---';
SELECT p, distinctJSONPaths(j), distinctJSONPathsAndTypes(j)
FROM (
    SELECT 1 AS p, CAST('{"a":1}', 'JSON') AS j
    UNION ALL SELECT 1, CAST('{"b":2}', 'JSON')
    UNION ALL SELECT 2, CAST('{"c":3}', 'JSON')
    UNION ALL SELECT 2, CAST('{"a":"x"}', 'JSON'))
GROUP BY p ORDER BY p;

SELECT '--- empty JSON object ---';
SELECT distinctJSONPaths(CAST('{}', 'JSON'));
SELECT distinctJSONPathsAndTypes(CAST('{}', 'JSON'));

SELECT '--- Nullable(JSON) with NULL ---';
SELECT distinctJSONPaths(CAST(NULL AS Nullable(JSON)));
SELECT distinctJSONPathsAndTypes(CAST(NULL AS Nullable(JSON)));

SELECT '--- array of mixed types in one path ---';
SELECT distinctJSONPathsAndTypes(j) FROM (
    SELECT CAST('{"a":1}', 'JSON') AS j
    UNION ALL SELECT CAST('{"a":1.5}', 'JSON')
    UNION ALL SELECT CAST('{"a":"hi"}', 'JSON')
    UNION ALL SELECT CAST('{"a":true}', 'JSON')
    UNION ALL SELECT CAST('{"a":[1,2]}', 'JSON'));

SELECT '--- deep nesting ---';
SELECT distinctJSONPaths(j) FROM (
    SELECT CAST('{"root":{"level1":{"level2":{"level3":{"leaf":1}}}}}', 'JSON') AS j);
SELECT distinctJSONPathsAndTypes(j) FROM (
    SELECT CAST('{"root":{"level1":{"level2":{"level3":{"leaf":1}}}}}', 'JSON') AS j);

SELECT '--- error: non-JSON argument ---';
SELECT distinctJSONPaths(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT distinctJSONPathsAndTypes('string'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE distinct_json_paths_t;
