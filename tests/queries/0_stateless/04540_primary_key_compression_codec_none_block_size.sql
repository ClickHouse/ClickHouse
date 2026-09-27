-- Regression test: with primary_key_compression_codec = 'NONE', the primary index blocks must
-- honor primary_key_compress_block_size instead of being clamped to the fixed 1 MiB
-- (DBMS_DEFAULT_BUFFER_SIZE) buffer of the primary index file. If they were clamped, a table with
-- a large primary_key_compress_block_size would produce the same frames (and thus the same
-- primary.cidx size) as a table with a smaller one, and the size comparison below would return 0.

DROP TABLE IF EXISTS pk_none_big;
DROP TABLE IF EXISTS pk_none_small;

-- A 64 KiB String primary key stores ~64 KiB per granule, so 192 granules (index_granularity = 1)
-- build a ~12 MiB primary index while inserting only 192 rows: large enough to span many frames at
-- the smaller block size, small enough to stay well under the "Test runs too long" flaky-check limit.
CREATE TABLE pk_none_big (k String) ENGINE = MergeTree ORDER BY k
    SETTINGS compress_primary_key = 1, primary_key_compression_codec = 'NONE',
             primary_key_compress_block_size = 16777216, index_granularity = 1,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE pk_none_small (k String) ENGINE = MergeTree ORDER BY k
    SETTINGS compress_primary_key = 1, primary_key_compression_codec = 'NONE',
             primary_key_compress_block_size = 2097152, index_granularity = 1,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO pk_none_big SELECT leftPad(toString(number), 65536, '0') FROM numbers(192);
INSERT INTO pk_none_small SELECT leftPad(toString(number), 65536, '0') FROM numbers(192);

-- The 16 MiB block fits the whole index in a single frame, while the 2 MiB block needs several, and
-- every NONE frame carries a fixed number of service bytes (checksum + header). So the primary index
-- written with the larger block must be strictly smaller on disk (primary_key_size is the on-disk
-- primary.cidx file size). If the block size were clamped to the 1 MiB index file buffer, both
-- indexes would have identical framing and equal sizes.
SELECT (SELECT sum(primary_key_size) FROM system.parts WHERE database = currentDatabase() AND table = 'pk_none_big' AND active)
     < (SELECT sum(primary_key_size) FROM system.parts WHERE database = currentDatabase() AND table = 'pk_none_small' AND active);

-- The index must still be usable: a point lookup via the primary key and a full count.
SELECT count() FROM pk_none_big WHERE k = leftPad('100', 65536, '0');
SELECT count() FROM pk_none_big;

DROP TABLE pk_none_big;
DROP TABLE pk_none_small;
