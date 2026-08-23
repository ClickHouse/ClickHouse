-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

DROP TABLE IF EXISTS t_manual_timeout;

CREATE TABLE t_manual_timeout (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_timeout VALUES (1);
INSERT INTO t_manual_timeout VALUES (2);

-- Merges are stopped, so the scheduled merge never happens and SYNC MERGES has to give up
-- once max_execution_time has elapsed.
SYSTEM STOP MERGES t_manual_timeout;
SYSTEM SCHEDULE MERGE t_manual_timeout PARTS 'all_1_1_0', 'all_2_2_0';

SET max_execution_time = 1;
SYSTEM SYNC MERGES t_manual_timeout; -- { serverError TIMEOUT_EXCEEDED }
SET max_execution_time = DEFAULT;

SELECT 'timed out';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_timeout' AND active ORDER BY name;

DROP TABLE t_manual_timeout;
