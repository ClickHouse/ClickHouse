-- Tags: long

SET enable_json_type = 1;
SET allow_experimental_object_type = 0;

DROP TABLE IF EXISTS test_deep_merge_json;

CREATE TABLE test_deep_merge_json
(
    id UInt64,
    data JSON
)
ENGINE = MergeTree()
ORDER BY id;

-- Test 1: Basic deep merge
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": 1, "b": {"c": 2}}'),
(1, '{"b": {"d": 3}, "e": 4}'),
(1, '{"a": 5, "b": {"c": 6, "e": 7}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 2: Multiple keys
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"user": {"name": "Alice", "age": 25}}'),
(1, '{"user": {"email": "alice@example.com"}}'),
(2, '{"user": {"name": "Bob"}}'),
(2, '{"user": {"age": 30, "city": "NYC"}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id ORDER BY id;

-- Test 3: Array handling (arrays are replaced, not merged)
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"tags": ["a", "b"], "values": [1, 2]}'),
(1, '{"tags": ["c", "d"], "other": "data"}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 4: Deep nesting
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"level1": {"level2": {"level3": {"a": 1}}}}'),
(1, '{"level1": {"level2": {"level3": {"b": 2}, "other": 3}}}'),
(1, '{"level1": {"level2": {"level3": {"a": 4}}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 5: Type conflicts (last value wins)
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"value": 123}'),
(1, '{"value": "string"}'),
(1, '{"value": {"nested": true}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 6: Null handling
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": null, "b": 1}'),
(1, '{"a": 2, "b": null}'),
(1, '{"c": 3}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 7: Empty objects
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{}'),
(1, '{"a": 1}'),
(1, '{}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 8: Path overwriting
TRUNCATE TABLE test_deep_merge_json;
-- First insert: a.b.c = 1
-- Second insert: a.b = 2 (overwrites a.b.c)
-- Third insert: a.b.d = 3 (overwrites a.b = 2)
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": {"b": {"c": 1}}}'),
(1, '{"a": {"b": 2}}'),
(1, '{"a": {"b": {"d": 3}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 9: Deletion with $unset
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"user": {"name": "Alice", "email": "alice@example.com", "phone": "+1234"}}'),
(1, '{"user": {"phone": {"$unset": true}}}'),
(1, '{"user": {"email": "newalice@example.com"}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 10: Deletion and re-insertion
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": 1, "b": 2, "c": 3}'),
(1, '{"b": {"$unset": true}}'),
(1, '{"b": 4, "d": 5}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 11: Nested deletion
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"level1": {"level2": {"a": 1, "b": 2}, "c": 3}}'),
(1, '{"level1": {"level2": {"$unset": true}}}'),
(1, '{"level1": {"d": 4}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 12: MongoDB CDC simulation
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"_id": "123", "user": {"name": "Alice", "email": "alice@example.com", "preferences": {"theme": "dark", "notifications": true}}}'),
(1, '{"user": {"email": "alice.new@example.com", "preferences": {"notifications": {"$unset": true}}}}'),
(1, '{"user": {"phone": "+1234567890", "preferences": {"language": "en"}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 13: Object to primitive type conflicts
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"data": {"value": {"nested": true, "count": 1}}}'),
(1, '{"data": {"value": 42}}'),
(1, '{"data": {"value": {"different": "object"}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 14: Primitive to object type conflicts
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"field": 123}'),
(1, '{"field": {"converted": "to object"}}'),
(1, '{"field": {"converted": "updated", "additional": "key"}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 15: Multiple parallel paths
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"branch1": {"leaf1": 1}, "branch2": {"leaf2": 2}}'),
(1, '{"branch1": {"leaf3": 3}, "branch3": {"leaf4": 4}}'),
(1, '{"branch2": {"leaf2": 20, "leaf5": 5}, "branch3": {"leaf6": 6}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 16: Empty object merging with non-empty
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{}'),
(1, '{"a": {"b": {"c": 1}}}'),
(1, '{}'),
(1, '{"a": {"b": {"d": 2}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 17: Array replacement behavior in paths
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"config": {"items": [1, 2, 3], "settings": {"max": 10}}}'),
(1, '{"config": {"items": [4, 5], "settings": {"min": 0}}}'),
(1, '{"config": {"items": [], "settings": {"max": 20, "default": 5}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 18: Null value handling in paths
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": {"b": null, "c": 1}}'),
(1, '{"a": {"b": {"d": 2}, "c": null}}'),
(1, '{"a": {"b": {"e": 3}, "f": 4}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

-- Test 19: Multiple groups with path variations
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"path": {"a": 1}}'),
(1, '{"path": {"b": 2}}'),
(2, '{"path": {"a": {"nested": 10}}}'),
(2, '{"path": {"a": {"nested": 20}, "c": 3}}'),
(3, '{"different": {"structure": true}}'),
(3, '{"different": {"structure": {"now": "object"}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id ORDER BY id;

-- Test 20: Path collision resolution
TRUNCATE TABLE test_deep_merge_json;
INSERT INTO test_deep_merge_json VALUES
(1, '{"a": {"b": {"c": {"d": 1}}}}'),
(1, '{"a": {"b": {"c": 2}}}'),
(1, '{"a": {"b": {"c": {"e": 3}}}}'),
(1, '{"a": {"b": {"c": {"d": 4, "f": 5}}}}');

SELECT id, deepMergeJSON(data) AS merged FROM test_deep_merge_json GROUP BY id;

DROP TABLE test_deep_merge_json;