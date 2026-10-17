-- `MODIFY COLUMN ... REMOVE ALIAS` is a metadata-only operation, so implicit minmax indices must
-- be refreshed carefully: an expression alias loses its implicit index (the index files in the
-- existing parts were built over the alias expression and are stale once existing rows read the
-- physical default value), while a simple identifier alias, which never had an implicit index,
-- gains one as it becomes physical.

DROP TABLE IF EXISTS t_remove_alias_minmax;

CREATE TABLE t_remove_alias_minmax
(
    a UInt64,
    expr_alias UInt64 ALIAS a + 1,
    id_alias UInt64 ALIAS a
)
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_remove_alias_minmax SELECT number FROM numbers(100000);

-- The expression alias has an implicit index, the identifier alias does not.
SELECT 'before';
SELECT name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_remove_alias_minmax' AND name LIKE 'auto_minmax_index_%'
ORDER BY name;

ALTER TABLE t_remove_alias_minmax MODIFY COLUMN expr_alias REMOVE ALIAS;
ALTER TABLE t_remove_alias_minmax MODIFY COLUMN id_alias REMOVE ALIAS;

-- The stale index over the former expression alias is gone; the former identifier alias gained one.
SELECT 'after';
SELECT name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_remove_alias_minmax' AND name LIKE 'auto_minmax_index_%'
ORDER BY name;

-- Existing rows read the physical default value 0. A stale minmax index built over `a + 1`
-- (whose values are 1 .. 100000) would wrongly prune every granule for these predicates.
SELECT count() FROM t_remove_alias_minmax WHERE expr_alias = 0;
SELECT count() FROM t_remove_alias_minmax WHERE id_alias = 0;

DROP TABLE t_remove_alias_minmax;
