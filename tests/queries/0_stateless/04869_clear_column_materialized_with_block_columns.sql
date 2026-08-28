-- `CLEAR COLUMN` recomputes the MATERIALIZED columns derived from the cleared one. When
-- `_block_number` or `_block_offset` is enabled, the mutation carries an extra READ_COLUMN command
-- for that virtual column, which used to freeze the set of written columns before the recompute
-- stages existed, so the MATERIALIZED column silently kept its pre-clear value on a Wide part.
--
-- The settings are pinned because the runner randomizes all three, and the recompute is skipped only
-- when a block column is enabled AND the part is Wide - with the defaults the case is never reached.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_clear_both_block_columns;

CREATE TABLE t_clear_both_block_columns (x Int32, y Int32, mk Int32 MATERIALIZED x + 1)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_clear_both_block_columns (x, y) VALUES (1, 0);
SELECT x, mk FROM t_clear_both_block_columns;

ALTER TABLE t_clear_both_block_columns CLEAR COLUMN x IN PARTITION tuple();
SELECT x, mk FROM t_clear_both_block_columns;

DROP TABLE t_clear_both_block_columns;

-- Either block column on its own is enough to add the extra stage.
DROP TABLE IF EXISTS t_clear_block_number;

CREATE TABLE t_clear_block_number (x Int32, y Int32, mk Int32 MATERIALIZED x + 1)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_clear_block_number (x, y) VALUES (1, 0);
ALTER TABLE t_clear_block_number CLEAR COLUMN x IN PARTITION tuple();
SELECT x, mk FROM t_clear_block_number;

DROP TABLE t_clear_block_number;

DROP TABLE IF EXISTS t_clear_block_offset;

CREATE TABLE t_clear_block_offset (x Int32, y Int32, mk Int32 MATERIALIZED x + 1)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_clear_block_offset (x, y) VALUES (1, 0);
ALTER TABLE t_clear_block_offset CLEAR COLUMN x IN PARTITION tuple();
SELECT x, mk FROM t_clear_block_offset;

DROP TABLE t_clear_block_offset;

-- A cleared column that feeds no MATERIALIZED column must still not be recomputed into the part,
-- and the block columns must not make the mutation invent one.
DROP TABLE IF EXISTS t_clear_unrelated_block_columns;

CREATE TABLE t_clear_unrelated_block_columns (x Int32, y Int32, mk Int32 MATERIALIZED x + 1)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_clear_unrelated_block_columns (x, y) VALUES (1, 5);
ALTER TABLE t_clear_unrelated_block_columns CLEAR COLUMN y IN PARTITION tuple();
SELECT x, y, mk FROM t_clear_unrelated_block_columns;

DROP TABLE t_clear_unrelated_block_columns;

-- A DELETE coalesced with an UPDATE rewrites every surviving column, while the index dependency
-- puts the otherwise unchanged `b` in the readonly stage. `b` must still be written against the new
-- row set - dropping it from the written set would hardlink files holding the pre-delete rows.
DROP TABLE IF EXISTS t_delete_with_index_dependency;

CREATE TABLE t_delete_with_index_dependency
(
    id Int64,
    a Int64,
    b Int64,
    INDEX idx (a, b) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1,
         enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_delete_with_index_dependency VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300);

ALTER TABLE t_delete_with_index_dependency DELETE WHERE id = 2, UPDATE a = a + 1 WHERE 1;

SELECT id, a, b FROM t_delete_with_index_dependency ORDER BY id;
SELECT count(), count(b), sum(b) FROM t_delete_with_index_dependency;
CHECK TABLE t_delete_with_index_dependency SETTINGS check_query_single_value_result = 1;

DROP TABLE t_delete_with_index_dependency;
