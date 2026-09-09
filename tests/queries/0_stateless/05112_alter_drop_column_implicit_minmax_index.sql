-- Regression: an implicit minmax index of a scalar dotted-name column (`n.z`) survived a
-- `DROP COLUMN n` (or was renamed onto a live one by `RENAME COLUMN n.x TO n.z`), so the
-- next ALTER failed with `Logical error: 'Index with name auto_minmax_index_n.z already exists'`.
-- Nested members are stored as `Array(...)` and never get implicit indices, but a scalar
-- dotted-name column re-added after the group was dropped does.

DROP TABLE IF EXISTS t_implicit_orphan;

CREATE TABLE t_implicit_orphan (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_implicit_orphan VALUES (1, [10], [20]);

SELECT 'nested members have no implicit index', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

-- Drop the group and re-add the member as a scalar: it gets an implicit minmax index.
ALTER TABLE t_implicit_orphan DROP COLUMN n;
ALTER TABLE t_implicit_orphan ADD COLUMN `n.x` Int64;
SELECT 'scalar column got an index', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

ALTER TABLE t_implicit_orphan RENAME COLUMN `n.x` TO `n.z`;
SELECT 'the index follows the rename', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

-- The drop walks the whole `n.*` range and must take the implicit index with it.
ALTER TABLE t_implicit_orphan DROP COLUMN n;
SELECT 'no orphan after dropping the parent', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

-- The shape from the AST fuzzer: re-add the scalar and rename it onto the previous name
-- in one statement.
ALTER TABLE t_implicit_orphan (ADD COLUMN IF NOT EXISTS `n.x` Int64), (RENAME COLUMN `n.x` TO `n.z`)
    SETTINGS mutations_sync = 2;
SELECT 're-add and rename in one alter', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;
SELECT a, n.z FROM t_implicit_orphan ORDER BY a;
CHECK TABLE t_implicit_orphan;
DROP TABLE t_implicit_orphan;

-- A rename onto the name of an implicit index that outlives its column definition through a
-- `CLEAR COLUMN` of the parent must replace that index instead of duplicating the name.
CREATE TABLE t_implicit_clear (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_implicit_clear VALUES (1, [10], [20]);

ALTER TABLE t_implicit_clear DROP COLUMN n;
ALTER TABLE t_implicit_clear ADD COLUMN `n.x` Int64;
ALTER TABLE t_implicit_clear ADD COLUMN `n.w` Int64;

-- `CLEAR COLUMN n` keeps the column definitions, so the implicit indices stay.
ALTER TABLE t_implicit_clear (CLEAR COLUMN n) SETTINGS mutations_sync = 2;
SELECT 'clear keeps the indices', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_clear' ORDER BY name;

ALTER TABLE t_implicit_clear (ADD COLUMN IF NOT EXISTS `n.x` Int64), (RENAME COLUMN `n.x` TO `n.z`)
    SETTINGS mutations_sync = 2;
SELECT 'rename replaces the stale index', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_clear' ORDER BY name;
SELECT a, n.z, n.w FROM t_implicit_clear ORDER BY a;
CHECK TABLE t_implicit_clear;
DROP TABLE t_implicit_clear;
