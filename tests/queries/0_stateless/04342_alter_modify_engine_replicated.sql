-- Tags: zookeeper, no-shared-merge-tree
-- ^ no-shared-merge-tree: SharedMergeTree replaces ReplicatedMergeTree, so the rejection below,
--   which is specific to Replicated*MergeTree, does not apply there.

-- MODIFY ENGINE keeps the replication kind fixed. Changing the merge semantics of a Replicated table
-- needs to go through the replicated metadata log, otherwise replicas would diverge, so Replicated
-- tables are rejected until that is implemented. The main test (04340_alter_modify_engine) only uses
-- non-replicated tables and so cannot reach either guard.

SET allow_experimental_alter_modify_engine = 1;

DROP TABLE IF EXISTS t_replicated SYNC;
CREATE TABLE t_replicated (a UInt32, v UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04342_alter_modify_engine/t', 'r1')
    ORDER BY a;

-- Replicated source, whichever engine is requested.
ALTER TABLE t_replicated MODIFY ENGINE = ReplicatedReplacingMergeTree(v); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_replicated MODIFY ENGINE = ReplacingMergeTree(v); -- { serverError SUPPORT_IS_DISABLED }

SELECT 'replicated rejected', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_replicated';
DROP TABLE t_replicated SYNC;

-- The other direction: a non-replicated table cannot be turned into a Replicated one either.
DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_plain (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_plain MODIFY ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/04342_alter_modify_engine/x', 'r1', v); -- { serverError SUPPORT_IS_DISABLED }

SELECT 'plain unchanged', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_plain';
DROP TABLE t_plain;
