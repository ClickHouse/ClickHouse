-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS test_00961;

CREATE TABLE test_00961 (d Date, a String, b UInt8, x String, y Int8, z UInt32)
    ENGINE = MergeTree PARTITION BY d ORDER BY (a, b)
    SETTINGS index_granularity = 111,
    min_bytes_for_wide_part = 0,
    compress_marks = 0,
    compress_primary_key = 0,
    index_granularity_bytes = '10Mi',
    ratio_of_defaults_for_sparse_serialization = 1,
    serialization_info_version = 'basic',
    replace_long_file_name_to_hash = 0,
    auto_statistics_types = '';

INSERT INTO test_00961 VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

SELECT
    name,
    table,
    hash_of_all_files,
    hash_of_uncompressed_files,
    uncompressed_hash_of_compressed_files
FROM system.parts
WHERE table = 'test_00961' and database = currentDatabase();

DROP TABLE test_00961;
