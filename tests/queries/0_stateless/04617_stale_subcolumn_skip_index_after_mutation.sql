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
