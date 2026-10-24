-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree
-- no-parallel: enables a server-global failpoint.

-- A manually scheduled merge must survive its selected merge being abandoned before launch -- the
-- same drop the STOP MERGES race causes in production. Before the fix, the manual selector removed
-- the queue entry the moment it handed the merge out, so an abandoned launch lost the schedule and
-- SYSTEM SYNC MERGES then waited for the result forever.

DROP TABLE IF EXISTS t_sched_retain;

CREATE TABLE t_sched_retain (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_sched_retain VALUES (1);
INSERT INTO t_sched_retain VALUES (2);

-- Pause the background pool right after it selects the scheduled merge, before it is launched.
SYSTEM ENABLE FAILPOINT storage_merge_tree_abandon_selected_merge;
SYSTEM SCHEDULE MERGE t_sched_retain PARTS 'all_1_1_0', 'all_2_2_0';

-- Deterministically wait until the merge has been selected (and, without the fix, already removed
-- from the queue) and paused.
SYSTEM WAIT FAILPOINT storage_merge_tree_abandon_selected_merge PAUSE;

-- Resume: the selection is abandoned without launching. Without the fix the queue entry is now lost.
SYSTEM NOTIFY FAILPOINT storage_merge_tree_abandon_selected_merge;
SYSTEM DISABLE FAILPOINT storage_merge_tree_abandon_selected_merge;

-- With the fix the schedule is still queued and the merge now runs; without the fix the entry was
-- lost and this SYNC would hang until it times out.
SET max_execution_time = 60;
SYSTEM SYNC MERGES t_sched_retain;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_sched_retain' AND active ORDER BY name;

DROP TABLE t_sched_retain;
