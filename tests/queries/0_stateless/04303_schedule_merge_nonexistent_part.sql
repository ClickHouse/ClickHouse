-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

DROP TABLE IF EXISTS t_manual_missing;

CREATE TABLE t_manual_missing (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_missing VALUES (1);
INSERT INTO t_manual_missing VALUES (2);
INSERT INTO t_manual_missing VALUES (3);
INSERT INTO t_manual_missing VALUES (4);

-- A scheduled merge that references a part which does not exist and is not produced by any
-- earlier scheduled merge must be rejected immediately. Otherwise SYNC MERGES would wait for
-- it until max_execution_time (default 0 => effectively forever).
SYSTEM SCHEDULE MERGE t_manual_missing PARTS 'all_1_1_0', 'all_999_999_0'; -- { serverError BAD_ARGUMENTS }

-- A non-canonical but parseable spelling of an existing part ('all_1_1_0_0' is the mutation-0
-- form of 'all_1_1_0') must also be rejected: the name is matched as a string against the real
-- part name, so an inexact spelling would never match in the queue and SYNC MERGES would hang.
SYSTEM SCHEDULE MERGE t_manual_missing PARTS 'all_1_1_0_0', 'all_2_2_0'; -- { serverError BAD_ARGUMENTS }

-- The 'exists' clause must stay load-bearing: a merge over only existing parts is accepted.
SYSTEM SCHEDULE MERGE t_manual_missing PARTS 'all_1_1_0', 'all_2_2_0';

-- The 'producible' clause must stay load-bearing: chained back-to-back (no SYNC in between),
-- the second merge references the result 'all_1_2_1' of the first, which does not exist yet.
SYSTEM SCHEDULE MERGE t_manual_missing PARTS 'all_1_2_1', 'all_3_3_0';

SYSTEM SYNC MERGES t_manual_missing;

-- After the chain, 'all_1_2_1' has been consumed (covered by 'all_1_3_2'). It must not be
-- trusted just because it was once a scheduled-merge result: a covered part can never be
-- merged again, so referencing it must be rejected rather than left to hang SYNC MERGES.
SYSTEM SCHEDULE MERGE t_manual_missing PARTS 'all_1_2_1', 'all_4_4_0'; -- { serverError BAD_ARGUMENTS }

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_missing' AND active ORDER BY name;

DROP TABLE t_manual_missing;

-- Scheduling the same parts twice (before either merge runs) must be rejected: the first merge
-- consumes the inputs, so the second can never run and would block the queue. Merges are stopped
-- so both schedules see the inputs as still active.
DROP TABLE IF EXISTS t_manual_dup;

CREATE TABLE t_manual_dup (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_dup VALUES (1);
INSERT INTO t_manual_dup VALUES (2);

SYSTEM STOP MERGES t_manual_dup;
SYSTEM SCHEDULE MERGE t_manual_dup PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE t_manual_dup PARTS 'all_1_1_0', 'all_2_2_0'; -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_manual_dup;
SYSTEM SYNC MERGES t_manual_dup;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_dup' AND active ORDER BY name;

DROP TABLE t_manual_dup;

-- A merge whose parts are in different partitions can never be selected, so it must be rejected
-- rather than left to hang SYNC MERGES.
DROP TABLE IF EXISTS t_manual_parts;

CREATE TABLE t_manual_parts (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_parts VALUES (1, 10);
INSERT INTO t_manual_parts VALUES (2, 20);

SYSTEM SCHEDULE MERGE t_manual_parts PARTS '1_1_1_0', '2_2_2_0'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_manual_parts;

-- Parts must be listed in ascending order: lookupRange matches them positionally against the
-- parts (which are ascending), so an out-of-order list can never be selected and would hang.
DROP TABLE IF EXISTS t_manual_order;

CREATE TABLE t_manual_order (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';

INSERT INTO t_manual_order VALUES (1);
INSERT INTO t_manual_order VALUES (2);

SYSTEM SCHEDULE MERGE t_manual_order PARTS 'all_2_2_0', 'all_1_1_0'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_manual_order;
