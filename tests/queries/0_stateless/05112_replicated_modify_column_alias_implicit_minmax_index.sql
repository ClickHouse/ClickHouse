-- Tags: zookeeper, no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: the test lists the implicit indices of a table.

-- A replica applies a metadata `ALTER` through the ZooKeeper metadata diff rather than through
-- `AlterCommand`, and the diff carries no index list: implicit indices are recreated locally from
-- the columns and the table settings. A redefinition of an ALIAS over a real expression must not
-- recreate the same-named implicit index there either - the index files of the existing parts were
-- built for the previous definition, and nothing rewrites them - so the follower has to reproduce
-- the deliberate absence of the index. See `04892` for the local `ALTER` path.

DROP TABLE IF EXISTS t_05112_r1 SYNC;
DROP TABLE IF EXISTS t_05112_r2 SYNC;

CREATE TABLE t_05112_r1 (a UInt64, expr_alias UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05112', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

CREATE TABLE t_05112_r2 (a UInt64, expr_alias UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05112', 'r2') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_05112_r1 SELECT number FROM numbers(100000);
SYSTEM SYNC REPLICA t_05112_r2;

SELECT 'the alias expression is indexed on both replicas';
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05112_r%' ORDER BY table, name;

-- A type change keeps the column an expression alias, so a predicate that compares the old and the
-- new definition of the column cannot tell the two apart: only the index list shows whether the
-- follower reused the files built before the change.
SELECT 'type change of an expression alias';
ALTER TABLE t_05112_r1 MODIFY COLUMN expr_alias UInt32 ALIAS a + 1;
SYSTEM SYNC REPLICA t_05112_r2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05112_r%' ORDER BY table, name;

-- The alias values are `a + 1`, so no row matches on either replica.
SELECT 'alias values', count() FROM t_05112_r1 WHERE expr_alias = 0;
SELECT 'alias values on the follower', count() FROM t_05112_r2 WHERE expr_alias = 0;

SELECT 'expression alias to physical';
ALTER TABLE t_05112_r1 MODIFY COLUMN expr_alias UInt32 DEFAULT 0;
SYSTEM SYNC REPLICA t_05112_r2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05112_r%' ORDER BY table, name;

-- The existing rows read the physical default `0` now. Reusing the index files of the alias
-- definition prunes them all away and returns `0` here.
SELECT 'physical values', count() FROM t_05112_r1 WHERE expr_alias = 0;
SELECT 'physical values on the follower', count() FROM t_05112_r2 WHERE expr_alias = 0;

DROP TABLE t_05112_r1 SYNC;
DROP TABLE t_05112_r2 SYNC;
