-- A skip index defined on a subcolumn (e.g. a Tuple element `t.a`) must be rebuilt (or dropped)
-- when a mutation changes the on-disk representation of that subcolumn. On wide parts the
-- skp_idx_* files are hardlinked from the source part, so a missed rebuild leaves the index holding
-- bytes serialized under the old subcolumn value/type, producing silent wrong results.

-- Case 1: ALTER MODIFY COLUMN of the parent (READ_COLUMN mutation path).
DROP TABLE IF EXISTS t_stale_idx_modify;
CREATE TABLE t_stale_idx_modify (id UInt32, t Tuple(a Int32, b String), INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_idx_modify SELECT number, (number, 'x') FROM numbers(16);

-- Same-width reinterpretation Int32 -> Float32; an ordinary full mutation.
ALTER TABLE t_stale_idx_modify MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 2;

SELECT count() FROM t_stale_idx_modify WHERE t.a >= 1;
SELECT count() FROM t_stale_idx_modify WHERE t.a >= 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_stale_idx_modify;

-- Case 2: ALTER UPDATE of the parent column.
DROP TABLE IF EXISTS t_stale_idx_update;
CREATE TABLE t_stale_idx_update (id UInt32, t Tuple(a Int32, b String), INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_idx_update SELECT number, (0, 'x') FROM numbers(16);

ALTER TABLE t_stale_idx_update UPDATE t = (id + 100, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_stale_idx_update WHERE t.a >= 105;
SELECT count() FROM t_stale_idx_update WHERE t.a >= 105 SETTINGS use_skip_indexes = 0;

DROP TABLE t_stale_idx_update;

-- Case 3: ALTER CLEAR COLUMN of the parent column (resets the subcolumn to its default).
DROP TABLE IF EXISTS t_stale_idx_clear;
CREATE TABLE t_stale_idx_clear (id UInt32, t Tuple(a Int32, b String), INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_idx_clear SELECT number, (number + 1, 'x') FROM numbers(16);

ALTER TABLE t_stale_idx_clear CLEAR COLUMN t SETTINGS mutations_sync = 2;

SELECT count() FROM t_stale_idx_clear WHERE t.a = 0;
SELECT count() FROM t_stale_idx_clear WHERE t.a = 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_stale_idx_clear;
