-- A skip index defined on a subcolumn (e.g. a Tuple element `t.a`) must be rebuilt when
-- ALTER MODIFY COLUMN of the parent changes the subcolumn's on-disk representation. On wide parts
-- the skp_idx_* files are hardlinked from the source part, so a missed rebuild leaves the index
-- holding bytes serialized under the old subcolumn type, producing silent wrong results.

DROP TABLE IF EXISTS t_stale_idx_modify;
CREATE TABLE t_stale_idx_modify (id UInt32, t Tuple(a Int32, b String), INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_idx_modify SELECT number, (number, 'x') FROM numbers(16);

-- Same-width reinterpretation Int32 -> Float32; an ordinary full mutation.
ALTER TABLE t_stale_idx_modify MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 2;

-- The index must agree with a scan that ignores skip indexes.
SELECT count() FROM t_stale_idx_modify WHERE t.a >= 1;
SELECT count() FROM t_stale_idx_modify WHERE t.a >= 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_stale_idx_modify;
