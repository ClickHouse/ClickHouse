-- A skip index, projection or TTL over a subcolumn (e.g. a Tuple element `t.a`) must be rebuilt (or dropped)
-- when a mutation changes that subcolumn through its column. On wide parts the files of an index are
-- hardlinked from the source part, so a missed rebuild leaves an index describing the old values.

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

-- Case 4: with alter_column_secondary_index_mode = 'throw', an ALTER of the parent of an explicit
-- index's subcolumn must be rejected synchronously (as it is for an index on a whole column),
-- instead of silently rebuilding/dropping the index during the mutation.
DROP TABLE IF EXISTS t_subcolumn_index_throw;
CREATE TABLE t_subcolumn_index_throw (id UInt32, t Tuple(a Int32, b String), INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, alter_column_secondary_index_mode = 'throw';

INSERT INTO t_subcolumn_index_throw SELECT number, (number, 'x') FROM numbers(4);

ALTER TABLE t_subcolumn_index_throw MODIFY COLUMN t Tuple(a Float32, b String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_subcolumn_index_throw CLEAR COLUMN t; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_subcolumn_index_throw;

-- Case 5: a projection whose sort key is a subcolumn of the altered column. Its own primary index
-- stores the sort key as raw bytes, so an ALTER MODIFY that changes the subcolumn type must rebuild
-- the projection; otherwise range analysis over the projection mis-prunes granules (wrong results).
DROP TABLE IF EXISTS t_stale_projection_modify;
CREATE TABLE t_stale_projection_modify (id UInt32, t Tuple(a Int32, b String), PROJECTION p (SELECT * ORDER BY t.a))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_projection_modify SELECT number, (number, 'x') FROM numbers(16);

ALTER TABLE t_stale_projection_modify MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 2;

SELECT count() FROM t_stale_projection_modify WHERE t.a >= 5 SETTINGS optimize_use_projections = 1;
SELECT count() FROM t_stale_projection_modify WHERE t.a >= 5 SETTINGS optimize_use_projections = 0;

DROP TABLE t_stale_projection_modify;

-- Case 6: a rows TTL defined on a subcolumn must be recalculated when the parent column is updated.
-- After moving the subcolumn value into the past, the rows must expire (as they do for a whole column).
DROP TABLE IF EXISTS t_stale_ttl_update;
CREATE TABLE t_stale_ttl_update (id UInt32, t Tuple(a DateTime, b String))
ENGINE = MergeTree ORDER BY id
TTL t.a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_ttl_update SELECT number, (now() + INTERVAL 1 YEAR, 'x') FROM numbers(4);

ALTER TABLE t_stale_ttl_update UPDATE t = (now() - INTERVAL 1 YEAR, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_stale_ttl_update;

DROP TABLE t_stale_ttl_update;

-- Case 7: an aggregate projection with a WHERE on a column must be rebuilt when ALTER MODIFY COLUMN
-- changes that column's type in a way that flips the filter. The projection stores a subset/aggregate
-- decided at build time; without a rebuild the stale aggregate is served (wrong result).
DROP TABLE IF EXISTS t_stale_projection_where;
CREATE TABLE t_stale_projection_where (id UInt64, x Int64, PROJECTION p (SELECT sum(id) WHERE x < 0))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- All x are > Int32 max and positive, so WHERE x < 0 matches nothing at build time (stored sum = 0).
INSERT INTO t_stale_projection_where SELECT number, toInt64(3000000000) + number FROM numbers(100);

-- Int64 -> Int32 wraps every value negative, so WHERE x < 0 now matches all rows.
ALTER TABLE t_stale_projection_where MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT sum(id) FROM t_stale_projection_where WHERE x < 0 SETTINGS optimize_use_projections = 1;
SELECT sum(id) FROM t_stale_projection_where WHERE x < 0 SETTINGS optimize_use_projections = 0;

DROP TABLE t_stale_projection_where;

-- Case 8: an index over an expression of a subcolumn (`t.a + 1`) keeps its stored expression name when it is
-- rebuilt, so the mutation does not fail with NOT_FOUND_COLUMN_IN_BLOCK.
DROP TABLE IF EXISTS t_expr_subcolumn_index;
CREATE TABLE t_expr_subcolumn_index (id UInt32, t Tuple(a Int32, b String), INDEX idx (t.a + 1) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_expr_subcolumn_index SELECT number, (number, 'x') FROM numbers(16);

ALTER TABLE t_expr_subcolumn_index UPDATE t = tuple(t.a, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2;
SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2 SETTINGS use_skip_indexes = 0;

ALTER TABLE t_expr_subcolumn_index MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 2;

SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2;
SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2 SETTINGS use_skip_indexes = 0;

DROP TABLE t_expr_subcolumn_index;

-- Case 9: an index over a subcolumn and another column (`t.a + y`) reads the other input too when it is rebuilt.
DROP TABLE IF EXISTS t_index_subcolumn_plus_other;
CREATE TABLE t_index_subcolumn_plus_other (id UInt32, t Tuple(a Int32, b String), y Int32, INDEX idx (t.a + y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_index_subcolumn_plus_other SELECT number, (number, 'x'), number FROM numbers(16);

ALTER TABLE t_index_subcolumn_plus_other UPDATE t = tuple(t.a + 1, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_index_subcolumn_plus_other WHERE t.a + y >= 2;
SELECT count() FROM t_index_subcolumn_plus_other WHERE t.a + y >= 2 SETTINGS use_skip_indexes = 0;

DROP TABLE t_index_subcolumn_plus_other;

-- Case 10: when one UPDATE changes both inputs, the index is rebuilt from the updated `t`, not from the subcolumn
-- of the source part.
DROP TABLE IF EXISTS t_idx_both_inputs;
CREATE TABLE t_idx_both_inputs (id UInt64, t Tuple(a Int32), y Int32, s String, INDEX idx (t.a + y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;

INSERT INTO t_idx_both_inputs SELECT number, tuple(toInt32(0)), 0, 'payload' FROM numbers(16);

ALTER TABLE t_idx_both_inputs UPDATE t = tuple(100), y = 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_idx_both_inputs WHERE t.a + y > 50 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_idx_both_inputs WHERE t.a + y > 50 SETTINGS use_skip_indexes = 0;

DROP TABLE t_idx_both_inputs;

-- Case 11: a column TTL on `t` resets it, so MATERIALIZE TTL rebuilds an index over `t.a + y`: the mutation reads
-- `y` as well, and the rebuilt index describes the reset value.
DROP TABLE IF EXISTS t_ttl_index;
CREATE TABLE t_ttl_index
(
    id UInt64,
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    y Int32,
    payload String,
    INDEX idx (t.a + y) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_index;
INSERT INTO t_ttl_index VALUES (1, '2000-01-01 00:00:00', tuple(100), 7, 'payload');

ALTER TABLE t_ttl_index MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT t, y FROM t_ttl_index;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 0;

-- The same when an ordinary UPDATE of the TTL source makes the TTL reset `t`.
DROP TABLE IF EXISTS t_ttl_index_update;
CREATE TABLE t_ttl_index_update
(
    id UInt64,
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    y Int32,
    payload String,
    INDEX idx (t.a + y) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_index_update;
INSERT INTO t_ttl_index_update VALUES (1, '2100-01-01 00:00:00', tuple(100), 7, 'payload');

ALTER TABLE t_ttl_index_update UPDATE d = toDateTime('2000-01-01 00:00:00') WHERE 1 SETTINGS mutations_sync = 2;

SELECT t, y FROM t_ttl_index_update;
SELECT count() FROM t_ttl_index_update WHERE t.a + y = 7 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_index_update WHERE t.a + y = 7 SETTINGS use_skip_indexes = 0;

DROP TABLE t_ttl_index;
DROP TABLE t_ttl_index_update;

-- Case 12: a TTL on a subcolumn is evaluated through getSubcolumn, but the TTL infos stored in a part are keyed by
-- the original expression (`ts.d + toIntervalDay(1)`), so parts written by older versions keep matching their rule.
DROP TABLE IF EXISTS t_ttl_subcolumn_key;
CREATE TABLE t_ttl_subcolumn_key (id UInt64, ts Tuple(d DateTime), v String)
ENGINE = MergeTree ORDER BY id
TTL ts.d + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1)), ts.d + INTERVAL 1 DAY DELETE WHERE id < 3
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_ttl_subcolumn_key SELECT number, tuple(toDateTime('2000-01-01 00:00:00', 'UTC')), 'x' FROM numbers(10);

OPTIMIZE TABLE t_ttl_subcolumn_key FINAL;

SELECT recompression_ttl_info.expression, default_compression_codec
FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_subcolumn_key' AND active;

SELECT count() FROM t_ttl_subcolumn_key;

DROP TABLE t_ttl_subcolumn_key;

-- Case 13: MATERIALIZE INDEX in the same mutation that writes the column of an index input builds the index from
-- the new value of the column, not from the subcolumn of the source part.
DROP TABLE IF EXISTS t_materialize_index_update;
CREATE TABLE t_materialize_index_update (id UInt64, t Tuple(a Int32), y Int32, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_materialize_index_update SELECT number, tuple(toInt32(0)), 0, 'p' FROM numbers(16);
ALTER TABLE t_materialize_index_update ADD INDEX idx (t.a + y) TYPE minmax GRANULARITY 1;
ALTER TABLE t_materialize_index_update UPDATE t = tuple(100), y = 1 WHERE 1, MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT count() FROM t_materialize_index_update WHERE t.a + y > 50;
SELECT count() FROM t_materialize_index_update WHERE t.a + y > 50 SETTINGS use_skip_indexes = 0;

DROP TABLE t_materialize_index_update;

-- The same when the column is a MATERIALIZED column recalculated by the mutation.
DROP TABLE IF EXISTS t_materialize_index_column;
CREATE TABLE t_materialize_index_column (id UInt64, x Int32, m Tuple(a Int32) MATERIALIZED tuple(x), payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_materialize_index_column (id, x, payload) SELECT number, 0, 'p' FROM numbers(16);
ALTER TABLE t_materialize_index_column MODIFY COLUMN m Tuple(a Int32) MATERIALIZED tuple(x + 100);
ALTER TABLE t_materialize_index_column ADD INDEX idx (m.a) TYPE minmax GRANULARITY 1;
ALTER TABLE t_materialize_index_column MATERIALIZE COLUMN m, MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT count() FROM t_materialize_index_column WHERE m.a > 50;
SELECT count() FROM t_materialize_index_column WHERE m.a > 50 SETTINGS use_skip_indexes = 0;

DROP TABLE t_materialize_index_column;
