-- Dropping the target of a queued recompression is allowed: a `RECOMPRESS COLUMN` only re-serializes
-- the data streams of its own target, and it is skipped for every part once that column is no longer
-- stored. `ALTER TABLE ... DROP COLUMN` is a barrier command, so on a non-replicated table it waits
-- for the queued recompression to finish first; a replica that applies the metadata change earlier
-- skips the recompression instead.

-- The recompression must not rewrite the part as a whole and pick up the meanwhile-changed codec of
-- a surviving column. Full part storage is pinned (`min_bytes_for_full_part_storage` may be
-- randomized in tests): a packed part does not support in-place recompression and legitimately
-- rewrites every column, which is not what this test is about.
DROP TABLE IF EXISTS t_recompress_dropped_target;

CREATE TABLE t_recompress_dropped_target
(
    id UInt64,
    a String CODEC(NONE),
    b String CODEC(NONE),
    c String CODEC(NONE)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

INSERT INTO t_recompress_dropped_target
SELECT number, repeat('a', 100), toString(number), toString(number) FROM numbers(10000);

ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN b SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_dropped_target MODIFY COLUMN a String CODEC(ZSTD);
ALTER TABLE t_recompress_dropped_target DROP COLUMN b SETTINGS mutations_sync = 0;

-- Waits for all prior mutations. The old bug sent the dropped target through a whole-part rewrite and
-- recompressed `a` as an unintended side effect.
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN c SETTINGS mutations_sync = 2;

SELECT 'dropped target does not rewrite surviving column', sum(data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_dropped_target' AND active AND column = 'a';

DROP TABLE t_recompress_dropped_target;
