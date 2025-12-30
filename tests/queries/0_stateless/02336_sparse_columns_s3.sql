-- Tags: no-parallel, no-fasttest, no-object-storage

DROP TABLE IF EXISTS t_sparse_s3;

CREATE TABLE t_sparse_s3 (id UInt32, cond UInt8, s String)
engine = MergeTree ORDER BY id
settings ratio_of_defaults_for_sparse_serialization = 0.01, storage_policy = 's3_cache',
min_bytes_for_wide_part = 0, min_compress_block_size = 1, serialization_info_version = 'basic',
index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_sparse_s3 SELECT 1, number % 2, '' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 2, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 3, number % 2, '' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 4, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 5, number % 2, '' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 6, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 7, number % 2, '' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 8, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 9, number % 2, '' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 10, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 11, number % 2, '' FROM numbers(8000);
INSERT INTO t_sparse_s3 SELECT 12, number % 2, 'foo' FROM numbers(192);
INSERT INTO t_sparse_s3 SELECT 13, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 14, number % 2, 'foo' FROM numbers(8192);
INSERT INTO t_sparse_s3 SELECT 15, number % 2, '' FROM numbers(24576);
INSERT INTO t_sparse_s3 SELECT 16, number % 2, 'foo' FROM numbers(4730);
INSERT INTO t_sparse_s3 SELECT 17, number % 2, 'foo' FROM numbers(3462);
INSERT INTO t_sparse_s3 SELECT 18, number % 2, '' FROM numbers(24576);

OPTIMIZE TABLE t_sparse_s3 FINAL;

SELECT serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_s3' AND active AND column = 's'
AND database = currentDatabase();

SET max_threads = 1;

SELECT count() FROM t_sparse_s3
PREWHERE cond
WHERE id IN (1, 3, 5, 7, 9, 11, 13, 15, 17)
AND NOT ignore(s);

DROP TABLE t_sparse_s3;
