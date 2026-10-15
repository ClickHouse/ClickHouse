DROP TABLE IF EXISTS test_json_cached_serialization_compact_wrapped;

CREATE TABLE test_json_cached_serialization_compact_wrapped
(
    json Tuple(data JSON(max_dynamic_paths = 0)),
    variant_json Variant(UInt64, JSON(stored UInt64))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO test_json_cached_serialization_compact_wrapped VALUES (tuple('{"unstored":42}'), '{"stored":43}');

SELECT part_type
FROM system.parts
WHERE database = currentDatabase()
    AND table = 'test_json_cached_serialization_compact_wrapped'
    AND active;

SELECT json.data.unstored.:Int64
FROM test_json_cached_serialization_compact_wrapped;

SELECT variant_json.`JSON(stored UInt64)`.stored
FROM test_json_cached_serialization_compact_wrapped;

DROP TABLE test_json_cached_serialization_compact_wrapped;
