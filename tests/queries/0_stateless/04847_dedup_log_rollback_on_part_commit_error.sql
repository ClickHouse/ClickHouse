-- Tags: no-parallel
-- no-parallel - the failpoint is global server state and would fail concurrent MergeTree inserts.

DROP TABLE IF EXISTS t_dedup_rollback;

CREATE TABLE t_dedup_rollback (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS non_replicated_deduplication_window = 100;

-- An insert that publishes its block ID into the deduplication log but then fails to commit
-- the part must unpublish the block ID, so a client retry of the same insert is not
-- wrongly deduplicated against a part that never became active.
SYSTEM ENABLE FAILPOINT merge_tree_sink_fail_part_commit_after_dedup;
INSERT INTO t_dedup_rollback VALUES (1); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_sink_fail_part_commit_after_dedup;

-- The retry must insert the data.
INSERT INTO t_dedup_rollback VALUES (1);
SELECT count() FROM t_dedup_rollback;

-- A repeated insert of the same block is now deduplicated as usual.
INSERT INTO t_dedup_rollback VALUES (1);
SELECT count() FROM t_dedup_rollback;

DROP TABLE t_dedup_rollback;
