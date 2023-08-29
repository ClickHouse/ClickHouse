-- Tags: no-parallel, no-fasttest, no-s3-storage

DROP TABLE IF EXISTS t_s3_compressed_blocks;

CREATE TABLE t_s3_compressed_blocks (id UInt64, s String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_cache',
min_bytes_for_wide_part = 0;

INSERT INTO t_s3_compressed_blocks SELECT number, randomPrintableASCII(128) from numbers(57344);

SET max_threads = 1;
SELECT count() FROM t_s3_compressed_blocks WHERE NOT ignore(s);

DROP TABLE t_s3_compressed_blocks;
