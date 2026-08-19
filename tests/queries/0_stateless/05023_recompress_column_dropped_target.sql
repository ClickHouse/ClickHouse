-- A queued recompression whose target is dropped must not force a whole-part rewrite of
-- surviving columns with their newly changed codecs.
DROP TABLE IF EXISTS t_recompress_dropped_target;

CREATE TABLE t_recompress_dropped_target
(
    id UInt64,
    a Float64 CODEC(NONE),
    b String CODEC(NONE),
    c String CODEC(NONE)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0;

INSERT INTO t_recompress_dropped_target
SELECT number, number + 0.12345, toString(number), toString(number) FROM numbers(10000);

-- Keep this mutation queued, then remove its target and change another column to a lossy codec.
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN b SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_dropped_target MODIFY COLUMN a Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_dropped_target DROP COLUMN b SETTINGS mutations_sync = 0;

-- Let the queued mutations run. This final recompression waits for all prior mutations without
-- rewriting `a`; the old bug sent the dropped target through a whole-part rewrite and made `a`
-- lossy as an unintended side effect.
ALTER TABLE t_recompress_dropped_target MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 1;
ALTER TABLE t_recompress_dropped_target RECOMPRESS COLUMN c SETTINGS mutations_sync = 2;

SELECT 'dropped target does not rewrite surviving column', countIf(a != id + 0.12345) = 0
FROM t_recompress_dropped_target;

DROP TABLE t_recompress_dropped_target;
