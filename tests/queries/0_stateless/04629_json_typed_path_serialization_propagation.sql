DROP TABLE IF EXISTS test_no_propagation;
DROP TABLE IF EXISTS test_with_propagation;

CREATE TABLE test_no_propagation
(
    j JSON(s String, t Tuple(v String), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    string_serialization_version = 'with_size_stream',
    propagate_types_serialization_versions_to_nested_types = false;

CREATE TABLE test_with_propagation AS test_no_propagation
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    string_serialization_version = 'with_size_stream',
    propagate_types_serialization_versions_to_nested_types = true;

INSERT INTO test_no_propagation
SELECT CAST(if(number = 0, '{"s":"value","t":{"v":"nested"}}', '{}'), 'JSON(s String, t Tuple(v String), max_dynamic_paths = 0)')
FROM numbers(10);

INSERT INTO test_with_propagation SELECT * FROM test_no_propagation;

SELECT
    table,
    arraySort(arrayFilter(stream -> stream IN ('j.s.size', 'j.t%2Ev.size'), substreams)) AS size_streams
FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('test_no_propagation', 'test_with_propagation') AND active
ORDER BY table;

SELECT count(), countIf(j.s = 'value'), countIf(j.t.v = 'nested') FROM test_no_propagation;
SELECT count(), countIf(j.s = 'value'), countIf(j.t.v = 'nested') FROM test_with_propagation;

DROP TABLE test_no_propagation;
DROP TABLE test_with_propagation;
