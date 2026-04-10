DROP TABLE IF EXISTS test_estimate_bytes;

CREATE TABLE test_estimate_bytes (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a;

INSERT INTO test_estimate_bytes SELECT number, toString(number), number / 1.0 FROM numbers(10000);

-- Both byte columns should be positive
SELECT count() FROM (
    EXPLAIN ESTIMATE SELECT * FROM test_estimate_bytes
) WHERE data_compressed_bytes > 0 AND data_uncompressed_bytes > 0;

-- Compressed should be less than or equal to uncompressed for typical data
SELECT data_compressed_bytes <= data_uncompressed_bytes FROM (
    EXPLAIN ESTIMATE SELECT * FROM test_estimate_bytes
);

-- Reading all rows should give larger or equal byte estimate than reading a subset
SELECT
    full.compressed >= partial.compressed AS compressed_scales,
    full.uncompressed >= partial.uncompressed AS uncompressed_scales
FROM
(
    SELECT data_compressed_bytes AS compressed, data_uncompressed_bytes AS uncompressed
    FROM (EXPLAIN ESTIMATE SELECT * FROM test_estimate_bytes)
) AS full,
(
    SELECT data_compressed_bytes AS compressed, data_uncompressed_bytes AS uncompressed
    FROM (EXPLAIN ESTIMATE SELECT * FROM test_estimate_bytes WHERE a < 5000)
) AS partial;

-- Selecting fewer columns should give smaller or equal byte estimate
SELECT
    all_cols.uncompressed >= one_col.uncompressed AS fewer_cols_fewer_bytes
FROM
(
    SELECT data_uncompressed_bytes AS uncompressed
    FROM (EXPLAIN ESTIMATE SELECT a, b, c FROM test_estimate_bytes)
) AS all_cols,
(
    SELECT data_uncompressed_bytes AS uncompressed
    FROM (EXPLAIN ESTIMATE SELECT a FROM test_estimate_bytes)
) AS one_col;

DROP TABLE test_estimate_bytes;
