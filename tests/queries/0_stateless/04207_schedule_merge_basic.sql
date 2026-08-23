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
