-- Tags: no-parallel
-- no-parallel - the failpoint is global server state and would fail concurrent MergeTree inserts.

DROP TABLE IF EXISTS t_dedup_full_window;

-- A window of one block: the insert that fails to commit below finds it already full.
CREATE TABLE t_dedup_full_window (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS non_replicated_deduplication_window = 1;

INSERT INTO t_dedup_full_window VALUES (1);

-- This insert publishes its block ID into the deduplication log and then fails to commit
-- the part. Rolling that publication back must restore the previous deduplication state
-- exactly: publishing into a full window must not cost the already committed block its
-- slot, or a retry of that unrelated insert would be accepted and duplicate its data.
SYSTEM ENABLE FAILPOINT merge_tree_sink_fail_part_commit_after_dedup;
INSERT INTO t_dedup_full_window VALUES (2); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_sink_fail_part_commit_after_dedup;

-- A retry of the committed insert is still deduplicated.
INSERT INTO t_dedup_full_window VALUES (1);
SELECT count() FROM t_dedup_full_window;

-- The same after the deduplication log is reloaded from disk, where the rolled back
-- publication must not consume a deduplication-window slot on replay either.
DETACH TABLE t_dedup_full_window;
ATTACH TABLE t_dedup_full_window;
INSERT INTO t_dedup_full_window VALUES (1);
SELECT count() FROM t_dedup_full_window;

-- The retry of the insert that failed to commit is accepted.
INSERT INTO t_dedup_full_window VALUES (2);
SELECT count() FROM t_dedup_full_window;

DROP TABLE t_dedup_full_window;
