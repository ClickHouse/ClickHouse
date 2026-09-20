-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

DROP TABLE IF EXISTS t_default;

CREATE TABLE t_default (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_default VALUES (1);
INSERT INTO t_default VALUES (2);

SYSTEM SCHEDULE MERGE t_default PARTS 'all_1_1_0', 'all_2_2_0'; -- { serverError BAD_ARGUMENTS }
SYSTEM SYNC MERGES t_default; -- { serverError BAD_ARGUMENTS }

SELECT 'ok';

DROP TABLE t_default;
