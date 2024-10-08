SET allow_experimental_dynamic_type=1;
SET allow_experimental_json_type=1;
SET allow_experimental_variant_type=1;

DROP TABLE IF EXISTS test_deep_nested_json;
CREATE TABLE test_deep_nested_json (i UInt16, d JSON) ENGINE = Memory;

INSERT INTO test_deep_nested_json VALUES (1, '{"level1": {"level2": {"level3": {"level4": {"level5": {"level6": {"level7": {"level8": {"level9": {"level10": "deep_value"}}}}}}}}}}');
INSERT INTO test_deep_nested_json VALUES (2, '{"level1": {"level2": {"level3": {"level4": {"level5": {"level6": {"level7": {"level8": {"level9": {"level10": "deep_array_value"}}}}}}}}}}');

SELECT * FROM test_deep_nested_json ORDER BY i;

SELECT '';
SELECT d::Dynamic d1, dynamicType(d1) FROM test_deep_nested_json ORDER BY i;
DROP TABLE test_deep_nested_json;
