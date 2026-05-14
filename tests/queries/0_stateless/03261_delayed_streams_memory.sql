-- Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_100_columns;

CREATE TABLE t_100_columns (id UInt64, c0 String, c1 String, c2 String, c3 String, c4 String, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String, c11 String, c12 String, c13 String, c14 String, c15 String, c16 String, c17 String, c18 String, c19 String, c20 String, c21 String, c22 String, c23 String, c24 String, c25 String, c26 String, c27 String, c28 String, c29 String, c30 String, c31 String, c32 String, c33 String, c34 String, c35 String, c36 String, c37 String, c38 String, c39 String, c40 String, c41 String, c42 String, c43 String, c44 String, c45 String, c46 String, c47 String, c48 String, c49 String, c50 String)
ENGINE = MergeTree
ORDER BY id PARTITION BY id % 50
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0, serialization_info_version = 'basic', max_compress_block_size = '1M', storage_policy = 's3_cache', auto_statistics_types = '';

SET max_insert_delayed_streams_for_parallel_write = 55;

INSERT INTO t_100_columns (id) SELECT number FROM numbers(100);

SYSTEM FLUSH LOGS query_log;

SELECT if (memory_usage < 300000000, 'Ok', format('Fail: memory usage {}', formatReadableSize(memory_usage)))
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO t_100_columns%' AND type = 'QueryFinish';

DROP TABLE t_100_columns;
