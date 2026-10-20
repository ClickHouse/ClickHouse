-- For `Nullable(JSON)`, `json.null` is the nested JSON path (type `Dynamic`), not the UInt8
-- null-map of the outer `Nullable`. The statistics part pruner must not mistake this real
-- subcolumn for the virtual `.null` key: the parts below contain no outer NULLs, so the
-- parent NULL count is 0, and treating `json.null = 1` as the null-map would prune every
-- part and return a wrong empty result.

SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS test_json_null_path_pruning;

CREATE TABLE test_json_null_path_pruning
(
    bucket UInt8,
    id UInt64,
    json Nullable(JSON) STATISTICS(basic)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY id
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Parts with the nested "null" path present and no outer NULLs: the NULL count is 0.
INSERT INTO test_json_null_path_pruning VALUES (0, 0, '{"null": 1}'), (0, 1, '{"null": 2}');
-- Parts without the nested "null" path.
INSERT INTO test_json_null_path_pruning VALUES (1, 2, '{"other": 3}'), (1, 3, '{"other": 4}');

SELECT 'Test 1: `json.null = 1` reads the nested path, parts must not be pruned';
SELECT id FROM test_json_null_path_pruning WHERE json.null = 1 ORDER BY id;
SELECT count() FROM test_json_null_path_pruning WHERE json.null = 1;

SELECT 'Test 2: a real `.null` subcolumn is not registered as a statistics pruning key';
SELECT countIf(explain LIKE '%Statistics%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_json_null_path_pruning WHERE json.null = 1);

SELECT 'Test 3: outer null-map semantics via `IS NULL` still prune by the NULL count';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 0/2%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_json_null_path_pruning WHERE json IS NULL);
SELECT count() FROM test_json_null_path_pruning WHERE json IS NULL;

DROP TABLE test_json_null_path_pruning;
