-- Regression test for the zero-copy NONE write path with a nested buffer that starts smaller than
-- the requested size. `openStreamFile` asks for `max_compress_block_size + 25` bytes, but with the
-- adaptive write buffer the file buffer starts at `adaptive_write_buffer_initial_size` and grows only
-- later; object storage writers (S3, Azure) behave the same way, starting at 1 MiB regardless of the
-- requested size. The direct path must stay off while the output buffer cannot expose a window for a
-- whole block plus the service bytes, so that the block size is honored in either case.
--
-- Here the initial buffer is exactly the block size, i.e. one service prefix short. A payload that
-- exactly fills a block must still occupy a single frame, so its on-disk size must equal the size of
-- the same payload written with a block size large enough to always hold it in one frame. If the
-- direct path took a window of `block_size - 25`, the exact-fit table would need a second frame and
-- be 25 bytes larger, and the comparison below would return 0.

DROP TABLE IF EXISTS none_adaptive_exact;
DROP TABLE IF EXISTS none_adaptive_huge;

-- 8192 UInt64 values with CODEC(NONE) are exactly 65536 bytes, so a single granule fills a
-- 65536-byte block exactly. min_columns_to_activate_adaptive_write_buffer = 1 turns the adaptive
-- write buffer on for this single-column table.
CREATE TABLE none_adaptive_exact (n UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_compress_block_size = 65536, max_compress_block_size = 65536,
             index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 65536;

CREATE TABLE none_adaptive_huge (n UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_compress_block_size = 1048576, max_compress_block_size = 1048576,
             index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 1048576;

INSERT INTO none_adaptive_exact SELECT number FROM numbers(8192);
INSERT INTO none_adaptive_huge  SELECT number FROM numbers(8192);

-- Equal only if the 65536-byte block still ends up in a single frame.
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'none_adaptive_exact' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'none_adaptive_huge' AND active);

-- The data must still be readable.
SELECT count(), sum(n) FROM none_adaptive_exact;

DROP TABLE none_adaptive_exact;
DROP TABLE none_adaptive_huge;
