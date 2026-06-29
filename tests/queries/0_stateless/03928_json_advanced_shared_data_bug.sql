DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `json` JSON(max_dynamic_paths = 1)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1, write_marks_for_substreams_in_compact_parts = 1, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced', object_shared_data_buckets_for_wide_part = 1, index_granularity = 100;

INSERT INTO test SELECT multiIf(number < 10, '{"a" : [{"b" : 42}]}', number < 20, '{}', '{"a" : [{"b" : 42}]}') from numbers(30);

SELECT * FROM test SETTINGS max_block_size=10;

DROP TABLE test;

