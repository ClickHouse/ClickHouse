-- Tags: no-parallel, no-shared-merge-tree, no-replicated-database
-- no-parallel: enables a global failpoint that the asserted server error depends on.
-- no-shared-merge-tree: the test is about the empty covering parts that plain `MergeTree` writes.
-- no-replicated-database: the test relies on the block numbers of a single, freshly created table.

-- `TRUNCATE` covers the visible parts with empty parts one level above them, renaming each to its final name
-- before committing. A commit that fails after that rename used to leave the part on disk under that name; a
-- later merge then produces a part of the same level over a wider block range, and the part loader finds a
-- pair of parts that neither contain one another nor are disjoint, so the table fails to attach with
-- "Part ... intersects previous part ...".

DROP TABLE IF EXISTS t_truncate_rollback;

-- Keep the rolled back and outdated parts on disk for the whole test.
CREATE TABLE t_truncate_rollback (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;

-- A background merge before the TRUNCATE would change the set of parts it covers.
SYSTEM STOP MERGES t_truncate_rollback;

INSERT INTO t_truncate_rollback SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_truncate_rollback SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_truncate_rollback SETTINGS async_insert = 0 VALUES (3);

SYSTEM ENABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;
TRUNCATE TABLE t_truncate_rollback; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;

-- Nothing was truncated.
SELECT count() FROM t_truncate_rollback;

-- A merge of the still active level 0 parts produces a part of the same level over a wider block range.
SYSTEM START MERGES t_truncate_rollback;
OPTIMIZE TABLE t_truncate_rollback FINAL;

DETACH TABLE t_truncate_rollback;
ATTACH TABLE t_truncate_rollback;

SELECT count() FROM t_truncate_rollback;

DROP TABLE t_truncate_rollback;

-- `MOVE PARTITION TO TABLE` renames the moved parts into the destination table before committing them there.
-- A destination commit that fails after that rename used to leave them on disk under their permanent names,
-- so a reload of the destination resurrected rows that the statement had failed to move.

DROP TABLE IF EXISTS t_move_rollback_src;
DROP TABLE IF EXISTS t_move_rollback_dst;

CREATE TABLE t_move_rollback_src (k UInt64) ENGINE = MergeTree ORDER BY k;

-- Keep whatever is left on disk for the whole test.
CREATE TABLE t_move_rollback_dst (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;

INSERT INTO t_move_rollback_src SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_move_rollback_src SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_move_rollback_src SETTINGS async_insert = 0 VALUES (3);

SYSTEM ENABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;
ALTER TABLE t_move_rollback_src MOVE PARTITION tuple() TO TABLE t_move_rollback_dst; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;

-- The rows are still in the source table, and the move did not land.
SELECT count() FROM t_move_rollback_src;
SELECT count() FROM t_move_rollback_dst;

DETACH TABLE t_move_rollback_dst;
ATTACH TABLE t_move_rollback_dst;

-- The reload must not resurrect the parts of the failed move.
SELECT count() FROM t_move_rollback_dst;

DROP TABLE t_move_rollback_src;
DROP TABLE t_move_rollback_dst;
