-- A queued recompression whose target is dropped must not force a whole-part rewrite of
-- surviving columns with their newly changed codecs.

-- Full part storage is pinned (`min_bytes_for_full_part_storage` may be randomized in tests):
-- a packed part does not support in-place recompression, so the queued `RECOMPRESS COLUMN b`
-- would rewrite the part as a whole, touching every column. This test exercises the in-place path.
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

-- Keep this mutation queued (stopping merges also stops mutations), then remove its target and
-- change another column's codec. Dropping the target of a pending recompression is allowed: the
-- recompression is skipped once the column is gone.
SYSTEM STOP MERGES t_recompress_dropped_target;

ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN b SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_dropped_target MODIFY COLUMN a String CODEC(ZSTD);
ALTER TABLE t_recompress_dropped_target DROP COLUMN b SETTINGS mutations_sync = 0;

-- Let the queued mutations run. This final recompression waits for all prior mutations without
-- rewriting `a`; the old bug sent the dropped target through a whole-part rewrite and recompressed
-- `a` as an unintended side effect.
SYSTEM START MERGES t_recompress_dropped_target;
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN c SETTINGS mutations_sync = 2;

SELECT 'dropped target does not rewrite surviving column', sum(data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_dropped_target' AND active AND column = 'a';

DROP TABLE t_recompress_dropped_target;
