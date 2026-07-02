-- Tags: no-fasttest, no-s3-storage, no-azure-blob-storage
-- Tag: no-azure-blob-storage - too slow
-- Tag: no-s3-storage - too slow
-- Regression test: SerializationObjectCombinedPath cached zero-based variant offsets
-- from temporary columns, poisoning the cache for SerializationSubObject which has
-- accumulated data from previous read ranges. The bug is exposed when
-- read_in_order_use_virtual_row=1 disables MergingSortedTransform batch passthrough,
-- forcing row-by-row insertFrom that uses the corrupted offsets.

SET enable_json_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS test_json_cache;
CREATE TABLE test_json_cache (id UInt64, json JSON(max_dynamic_paths=1000, a.b.c UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part=1, min_bytes_for_wide_part=1,
         object_serialization_version='v2',
         vertical_merge_algorithm_min_rows_to_activate=1,
         vertical_merge_algorithm_min_columns_to_activate=1;

-- Create 4 parts with different path combinations to produce a merged part
-- with multiple marks/granules and mixed typed+dynamic paths.
SYSTEM STOP MERGES test_json_cache;

INSERT INTO test_json_cache SELECT number, '{}' FROM numbers(5);
INSERT INTO test_json_cache SELECT number, toJSONString(map('a.b.c', number)) FROM numbers(5, 5);
INSERT INTO test_json_cache SELECT number, toJSONString(map('a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) FROM numbers(10, 5);
INSERT INTO test_json_cache SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) FROM numbers(20, 5);

-- Merge all 4 parts into one, producing a single part with multiple granules.
SYSTEM START MERGES test_json_cache;
OPTIMIZE TABLE test_json_cache FINAL SETTINGS mutations_sync=1;
SYSTEM STOP MERGES test_json_cache;

-- Add a second part with Array(UInt32) for a.b.d to test variant type diversity.
INSERT INTO test_json_cache SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', range(number % 5 + 1)::Array(UInt32), 'a.b.e', 'str_' || toString(number), 'd.a', number::UInt32, 'd.c', toDate(number))) FROM numbers(30, 5);

-- Now we have 2 parts. Reading with ORDER BY id triggers MergingSortedTransform.
-- With read_in_order_use_virtual_row=1, batch passthrough is disabled and
-- row-by-row insertFrom exposes the corrupted variant offsets in ^a.

-- Both queries must produce identical results.
SELECT 'combined_and_subobject_vrow1';
SELECT id, json.@a, json.^a FROM test_json_cache ORDER BY id SETTINGS read_in_order_use_virtual_row=1;
SELECT 'combined_and_subobject_vrow0';
SELECT id, json.@a, json.^a FROM test_json_cache ORDER BY id SETTINGS read_in_order_use_virtual_row=0;

DROP TABLE test_json_cache;
