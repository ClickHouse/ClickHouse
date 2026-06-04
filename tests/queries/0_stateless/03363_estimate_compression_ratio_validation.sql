DROP TABLE IF EXISTS test_table_for_estimate_compression_ratio;
CREATE TABLE test_table_for_estimate_compression_ratio (some_column Int64, other_column Int64) ENGINE = MergeTree ORDER BY some_column;

SELECT estimateCompressionRatio('lz4', 8192)(some_column, other_column) from test_table_for_estimate_compression_ratio; -- { serverError 42 }
SELECT estimateCompressionRatio('lz4', 8192, 2025)(some_column) from test_table_for_estimate_compression_ratio; -- { serverError 456 }
SELECT estimateCompressionRatio('zstd', 'lz4')(some_column) from test_table_for_estimate_compression_ratio; -- { serverError 457 }

SELECT estimateCompressionRatio('zstd', 8192)(some_column) from test_table_for_estimate_compression_ratio; -- positive case, should always be 0
-- order is not important
SELECT estimateCompressionRatio(8192, 'zstd')(some_column) from test_table_for_estimate_compression_ratio;

-- block_size_bytes > 0
SELECT estimateCompressionRatio(0)(some_column) from test_table_for_estimate_compression_ratio; -- { serverError BAD_QUERY_PARAMETER }
-- and now with some data
SELECT estimateCompressionRatio(0)(0); -- { serverError BAD_QUERY_PARAMETER }
