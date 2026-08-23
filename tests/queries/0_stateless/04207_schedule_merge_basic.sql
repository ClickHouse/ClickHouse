-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

DROP TABLE IF EXISTS t_manual;

CREATE TABLE t_manual (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual VALUES (1);
INSERT INTO t_manual VALUES (2);
INSERT INTO t_manual VALUES (3);
INSERT INTO t_manual VALUES (4);
INSERT INTO t_manual VALUES (5);
INSERT INTO t_manual VALUES (6);

SELECT 'before merges';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual' AND active ORDER BY name;

SYSTEM SCHEDULE MERGE t_manual PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE t_manual PARTS 'all_1_2_1', 'all_3_3_0';
SYSTEM SCHEDULE MERGE t_manual PARTS 'all_1_3_2', 'all_4_4_0';
SYSTEM SYNC MERGES t_manual;

SELECT '';
SELECT 'after merges';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual' AND active ORDER BY name;

DROP TABLE t_manual;

-- SYSTEM SYNC MERGES gives up once max_execution_time has elapsed instead of waiting forever
-- for a merge that cannot happen.
DROP TABLE IF EXISTS t_manual_timeout;

CREATE TABLE t_manual_timeout (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_timeout VALUES (1);
INSERT INTO t_manual_timeout VALUES (2);

SYSTEM STOP MERGES t_manual_timeout;
SYSTEM SCHEDULE MERGE t_manual_timeout PARTS 'all_1_1_0', 'all_2_2_0';

SET max_execution_time = 1;
SYSTEM SYNC MERGES t_manual_timeout; -- { serverError TIMEOUT_EXCEEDED }
SET max_execution_time = DEFAULT;

SELECT '';
SELECT 'timed out';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_timeout' AND active ORDER BY name;

SYSTEM START MERGES t_manual_timeout;
DROP TABLE t_manual_timeout;
