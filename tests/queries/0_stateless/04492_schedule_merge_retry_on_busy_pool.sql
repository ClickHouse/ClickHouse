-- Tags: no-parallel, no-shared-merge-tree, no-random-settings, no-random-merge-tree-settings
-- Tag no-parallel: uses a global failpoint
-- Tag no-shared-merge-tree: SYSTEM SCHEDULE MERGE / SYNC MERGES require the 'Manual' merge selector
-- Tag no-random-settings, no-random-merge-tree-settings: the test checks an exact resulting part name

-- Regression test: a manually scheduled merge that fails to be scheduled into the background pool
-- (e.g. the pool is full and trySchedule returns false) must be retried, not silently dropped.
-- Otherwise SYSTEM SYNC MERGES waits for a merge that never happens and hangs until it times out.

DROP TABLE IF EXISTS t_manual_retry;

CREATE TABLE t_manual_retry (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_retry VALUES (1);
INSERT INTO t_manual_retry VALUES (2);

-- The first attempt to schedule the manually selected merge is dropped (simulating a full pool).
SYSTEM ENABLE FAILPOINT mt_skip_scheduling_merge_once;

SYSTEM SCHEDULE MERGE t_manual_retry PARTS 'all_1_1_0', 'all_2_2_0';

-- Bound the wait so that, if the merge were lost, the test fails fast instead of hanging.
SET max_execution_time = 120;
SYSTEM SYNC MERGES t_manual_retry;

SYSTEM DISABLE FAILPOINT mt_skip_scheduling_merge_once;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_retry' AND active ORDER BY name;

DROP TABLE t_manual_retry;
