-- One thread compresses `b` right after `a`, so the 1 MiB window and long-range matching of `ZSTD(3, 20)` must not leak into `ZSTD(1)`:
-- the period of `b` (800 KB) is beyond the 512 KiB window of `ZSTD(1)`, so `b` stays incompressible.
DROP TABLE IF EXISTS t_zstd_pooled;
CREATE TABLE t_zstd_pooled (a UInt64 CODEC(ZSTD(3, 20)), b UInt64 CODEC(ZSTD(1)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 1048576, max_compress_block_size = 1048576;
INSERT INTO t_zstd_pooled SELECT number, intHash64(number % 100000 + 1) FROM numbers(1000000) SETTINGS max_insert_threads = 1, max_threads = 1, max_block_size = 1000000;
SELECT data_compressed_bytes > data_uncompressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_zstd_pooled' AND name = 'b';
DROP TABLE t_zstd_pooled;
