-- Tags: no-fasttest
SET enable_json_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS test_combined_mt_mixed;
CREATE TABLE test_combined_mt_mixed (id UInt64, json JSON(max_dynamic_paths=10)) ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part=1, min_bytes_for_wide_part=1;

INSERT INTO test_combined_mt_mixed VALUES (1, '{"a" : 42}');
INSERT INTO test_combined_mt_mixed VALUES (2, '{"a" : 0}');
INSERT INTO test_combined_mt_mixed VALUES (3, '{"a" : {"x" : 1, "y" : 2}}');
INSERT INTO test_combined_mt_mixed VALUES (4, '{"a" : {"x" : 3}}');
INSERT INTO test_combined_mt_mixed VALUES (5, '{"b" : 1}');
INSERT INTO test_combined_mt_mixed VALUES (6, '{}');

OPTIMIZE TABLE test_combined_mt_mixed FINAL;

SELECT id, json.@a, dynamicType(json.@a) FROM test_combined_mt_mixed ORDER BY id;
SELECT id, json.@a, json.^a, json.a FROM test_combined_mt_mixed ORDER BY id;

DROP TABLE test_combined_mt_mixed;
