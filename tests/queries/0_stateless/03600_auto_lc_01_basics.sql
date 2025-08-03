DROP TABLE IF EXISTS test_table;
SET allow_experimental_statistics = 1;

CREATE TABLE test_table
(
    id UInt64 STATISTICS(defaults, uniq),
    v1 String STATISTICS(defaults, uniq),
    v2 UInt64 STATISTICS(defaults, uniq),
    v3 String STATISTICS(defaults, uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    use_statistics_for_serialization_info = 1,
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    max_uniq_number_for_low_cardinality = 1000;

SYSTEM STOP MERGES test_table;

INSERT INTO test_table SELECT number, if (rand() % 100 = 0, 'foo', ''), rand() % 2, rand() % 2 FROM numbers(100000);
INSERT INTO test_table SELECT number, if (rand() % 100 = 0, 'bar', ''), rand() % 2 + 5, rand() % 2 + 5 FROM numbers(100000);

SELECT name, column, serialization_kind, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_table' AND active
ORDER BY name, column;

SELECT count() FROM test_table WHERE NOT ignore(*);
SELECT uniqExact(v1), uniqExact(v2), uniqExact(v3) FROM test_table WHERE NOT ignore(*);

SYSTEM START MERGES test_table;
OPTIMIZE TABLE test_table FINAL;

SELECT name, column, serialization_kind, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_table' AND active
ORDER BY name, column;

SELECT count() FROM test_table WHERE NOT ignore(*);
SELECT uniqExact(v1), uniqExact(v2), uniqExact(v3) FROM test_table WHERE NOT ignore(*);
