SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS t_dry_run;

CREATE TABLE t_dry_run (key UInt64, value String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dry_run', '1') ORDER BY key;

SYSTEM STOP MERGES t_dry_run;

INSERT INTO t_dry_run VALUES (1, 'a'), (2, 'b');
INSERT INTO t_dry_run VALUES (1, 'c'), (4, 'd');
INSERT INTO t_dry_run VALUES (5, 'e'), (6, 'f') (6, 'f');

SELECT 'parts before dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run' AND active ORDER BY name;

SELECT 'data before dry run';
SELECT * FROM t_dry_run ORDER BY key, value;

OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_0_0_0', 'all_1_1_0', 'all_2_2_0';
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_0_0_0', 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;
OPTIMIZE TABLE t_dry_run DRY RUN PARTS 'all_0_0_0', 'all_1_1_0', 'all_2_2_0' DEDUPLICATE BY key;

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

DROP TABLE t_dry_run;
