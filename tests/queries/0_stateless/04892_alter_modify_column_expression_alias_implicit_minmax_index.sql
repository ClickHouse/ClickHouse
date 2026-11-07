-- Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: the test lists the implicit indices of a table.

-- `MODIFY COLUMN` never rewrites the skip index files of the existing parts, so a transition into or
-- out of an ALIAS over a real expression must not re-create the same-named implicit index: it would
-- be reused over files built for the other definition and prune away matching rows.

DROP TABLE IF EXISTS t_04892;

CREATE TABLE t_04892 (a UInt64, expr_alias UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_04892 SELECT number FROM numbers(100000);

SELECT 'the alias expression is indexed';
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892' ORDER BY name;

-- The alias values are `a + 1`, so `expr_alias = 0` matches nothing while the column is an alias.
SELECT 'alias values', count() FROM t_04892 WHERE expr_alias = 0;

SELECT 'expression alias to physical';
ALTER TABLE t_04892 MODIFY COLUMN expr_alias UInt64 DEFAULT 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892' ORDER BY name;

-- The existing rows read the physical default `0` now. With the stale index re-created under the same
-- name this returned `0`, because the index files still held the `a + 1` value ranges.
SELECT 'physical values', count() FROM t_04892 WHERE expr_alias = 0;

-- An unrelated settings-only ALTER must not rehydrate the deliberately absent index: its files
-- still hold the alias-expression values until a rewrite creates replacement files.
SELECT 'unrelated setting alter';
ALTER TABLE t_04892 MODIFY SETTING enable_block_number_column = 1;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892' ORDER BY name;
SELECT 'physical values after unrelated setting alter', count() FROM t_04892 WHERE expr_alias = 0;

SELECT 'physical to expression alias';
ALTER TABLE t_04892 MODIFY COLUMN expr_alias UInt64 ALIAS a + 1;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892' ORDER BY name;
SELECT 'alias values again', count() FROM t_04892 WHERE expr_alias = 0;

-- A type change of an expression alias invalidates its index files just as well.
SELECT 'type change of an expression alias';
ALTER TABLE t_04892 MODIFY COLUMN expr_alias UInt32 ALIAS a + 1;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892' ORDER BY name;
SELECT 'alias values after the type change', count() FROM t_04892 WHERE expr_alias = 0;

-- A plain identifier alias never had an implicit index, so turning it physical may gain one: there
-- are no index files of the alias definition to become stale.
SELECT 'identifier alias to physical';
DROP TABLE IF EXISTS t_04892_id;

CREATE TABLE t_04892_id (a UInt64, id_alias UInt64 ALIAS a)
ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_04892_id SELECT number FROM numbers(100000);

SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892_id' ORDER BY name;
ALTER TABLE t_04892_id MODIFY COLUMN id_alias UInt64 DEFAULT 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_04892_id' ORDER BY name;
SELECT 'physical values', count() FROM t_04892_id WHERE id_alias = 0;

DROP TABLE t_04892;
DROP TABLE t_04892_id;
