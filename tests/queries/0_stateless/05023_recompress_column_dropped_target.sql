-- A queued recompression whose target is dropped must not force a whole-part rewrite of
-- surviving columns with their newly changed codecs.
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
    number_of_free_entries_in_pool_to_execute_mutation = 0;

INSERT INTO t_recompress_dropped_target
SELECT number, repeat('a', 100), toString(number), toString(number) FROM numbers(10000);

-- Keep this mutation queued, then remove its target and change another column's codec.
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN b SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_dropped_target MODIFY COLUMN a String CODEC(ZSTD);
ALTER TABLE t_recompress_dropped_target DROP COLUMN b SETTINGS mutations_sync = 0;

-- Let the queued mutations run. This final recompression waits for all prior mutations without
-- rewriting `a`; the old bug sent the dropped target through a whole-part rewrite and recompressed
-- `a` as an unintended side effect.
ALTER TABLE t_recompress_dropped_target MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 1;
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN c SETTINGS mutations_sync = 2;

SELECT 'dropped target does not rewrite surviving column', sum(data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_dropped_target' AND active AND column = 'a';

DROP TABLE t_recompress_dropped_target;
