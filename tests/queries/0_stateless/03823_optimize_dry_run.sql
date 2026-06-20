SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_dry_run;

CREATE TABLE t_dry_run (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

SYSTEM STOP MERGES t_dry_run;

INSERT INTO t_dry_run VALUES (1, 'a'), (2, 'b');
INSERT INTO t_dry_run VALUES (1, 'c'), (4, 'd');
INSERT INTO t_dry_run VALUES (5, 'e'), (6, 'f') (6, 'f');

SELECT 'parts before dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run' AND active ORDER BY name;

SELECT 'data before dry run';
SELECT * FROM t_dry_run ORDER BY key, value;

OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0';
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0' DEDUPLICATE;
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0' DEDUPLICATE BY key;

-- After DRY RUN, parts must remain unchanged: no merge committed.
SELECT 'parts after dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run' AND active ORDER BY name;

SELECT 'data after dry run';
SELECT * FROM t_dry_run ORDER BY key, value;

SYSTEM FLUSH LOGS query_log, part_log;
SELECT 'profile events';

SELECT
    query,
    query_kind,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergeSourceParts'],
    ProfileEvents['MergeWrittenRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'OPTIMIZE TABLE t_dry_run DRY RUN PARTS%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

SELECT 'part log should not cotain merge in dry run';
SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = 't_dry_run' AND event_type = 'MergeParts';

-- Error: non-existent part.
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'nonexistent_part'; -- { serverError BAD_DATA_PART_NAME }

-- Error: incompatible with FINAL.
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_1_1_0' FINAL; -- { serverError BAD_ARGUMENTS }

-- Error: incompatible with PARTITION.
OPTIMIZE TABLE t_dry_run PARTITION tuple() DRY RUN PARTS 'all_1_1_0'; -- { serverError BAD_ARGUMENTS }

-- Error: non-existent part.
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_0_0_0', 'all_4_4_0'; -- { serverError NO_SUCH_DATA_PART }

DROP TABLE t_dry_run;

-- Error: non-MergeTree engine.
DROP TABLE IF EXISTS t_dry_run_memory;
CREATE TABLE t_dry_run_memory (key UInt64) ENGINE = Memory;
OPTIMIZE TABLE t_dry_run_memory DRY RUN PARTS 'some_part'; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_dry_run_memory;
