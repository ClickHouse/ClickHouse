SELECT estimateCompressionRatio('lz4', 8192)(name, table) from system.columns; -- { serverError 42 }
SELECT estimateCompressionRatio('lz4', 8192, 2025)(name) from system.columns; -- { serverError 456 }
SELECT estimateCompressionRatio('zstd', 'lz4')(name) from system.columns; -- { serverError 457 }

DROP TABLE IF EXISTS test_table_for_estimate_compression_ratio;
CREATE TABLE test_table_for_estimate_compression_ratio (some_column Int64) ENGINE = MergeTree ORDER BY some_column;

SELECT estimateCompressionRatio('zstd', 8192)(some_column) from test_table_for_estimate_compression_ratio; -- positive case, should always be 0