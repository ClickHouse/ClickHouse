-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings

-- The implicit minmax indices created by `add_minmax_index_for_numeric_columns` also cover
-- `ALIAS` columns with a non-trivial body, so column matcher re-expansion inside that body
-- makes them schema-sensitive just like explicit indices: an unrelated `ADD COLUMN` changes the
-- effective index expression while existing parts keep index files built from the old one.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- REBUILD mode rebuilds the implicit index and keeps pruning correct';

DROP TABLE IF EXISTS t_implicit_minmax_rebuild;

CREATE TABLE t_implicit_minmax_rebuild
(
    a UInt64,
    d UInt64 ALIAS greatest(a, COLUMNS('^[ab]$') EXCEPT d)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 100,
    alter_column_secondary_index_mode = 'rebuild';

INSERT INTO t_implicit_minmax_rebuild SELECT number FROM numbers(1000);

-- Before the ALTER, `d` is `greatest(a, a)`, so the index over it stores the values of `a`.
SELECT count() FROM t_implicit_minmax_rebuild WHERE d = 999
SETTINGS force_data_skipping_indices = 'auto_minmax_index_d';

-- `ADD COLUMN b` extends the matcher inside the body of `d` to `greatest(a, a, b)`. The stored
-- index files describe the old expression, so they must be rebuilt: otherwise forced pruning
-- over the new expression would skip the granules that do match.
ALTER TABLE t_implicit_minmax_rebuild ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_implicit_minmax_rebuild' AND command LIKE '%auto_minmax_index_d%';

SELECT count() FROM t_implicit_minmax_rebuild WHERE d = 1999
SETTINGS force_data_skipping_indices = 'auto_minmax_index_d';

SELECT count() FROM t_implicit_minmax_rebuild WHERE d = 999
SETTINGS force_data_skipping_indices = 'auto_minmax_index_d';

DROP TABLE t_implicit_minmax_rebuild;

SELECT '-- THROW mode rejects the ALTER';

DROP TABLE IF EXISTS t_implicit_minmax_throw;

CREATE TABLE t_implicit_minmax_throw
(
    a UInt64,
    d UInt64 ALIAS greatest(a, COLUMNS('^[ab]$') EXCEPT d)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 100,
    alter_column_secondary_index_mode = 'throw';

INSERT INTO t_implicit_minmax_throw SELECT number FROM numbers(1000);

ALTER TABLE t_implicit_minmax_throw ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM t_implicit_minmax_throw WHERE d = 999
SETTINGS force_data_skipping_indices = 'auto_minmax_index_d';

DROP TABLE t_implicit_minmax_throw;

SELECT '-- an implicit index over an ordinary column is not affected';

DROP TABLE IF EXISTS t_implicit_minmax_ordinary;

CREATE TABLE t_implicit_minmax_ordinary
(
    a UInt64,
    s String
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 100,
    alter_column_secondary_index_mode = 'throw';

INSERT INTO t_implicit_minmax_ordinary SELECT number, toString(number) FROM numbers(1000);

-- Neither `ADD COLUMN` nor `RENAME COLUMN` changes the expression of `auto_minmax_index_a`,
-- so no ALTER is rejected and no rebuild mutation is queued.
ALTER TABLE t_implicit_minmax_ordinary ADD COLUMN b UInt64 DEFAULT 1000;
ALTER TABLE t_implicit_minmax_ordinary RENAME COLUMN a TO c;

SELECT count() FROM t_implicit_minmax_ordinary WHERE c = 999
SETTINGS force_data_skipping_indices = 'auto_minmax_index_c';

-- Dropping the column drops its implicit index as well; that must not be mistaken for a
-- changed expression (the index no longer exists to rebuild).
ALTER TABLE t_implicit_minmax_ordinary DROP COLUMN c;

SELECT count() FROM t_implicit_minmax_ordinary;

DROP TABLE t_implicit_minmax_ordinary;

-- The implicit indices over the persistent virtual columns `_block_number` / `_block_offset` are
-- the one case that keeps the virtual columns in scope while being re-resolved; that they survive
-- an unrelated column ALTER is covered by `04401_implicit_minmax_block_number_offset_alter_column`.
