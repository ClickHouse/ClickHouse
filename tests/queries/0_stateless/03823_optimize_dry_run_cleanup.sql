SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_dry_run_cleanup;

CREATE TABLE t_dry_run_cleanup (uid String, version UInt32, is_deleted UInt8)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY uid
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

SYSTEM STOP MERGES t_dry_run_cleanup;

INSERT INTO t_dry_run_cleanup VALUES ('a', 1, 0), ('b', 1, 0);
INSERT INTO t_dry_run_cleanup VALUES ('a', 2, 1), ('c', 1, 0);
INSERT INTO t_dry_run_cleanup VALUES ('b', 2, 1), ('d', 1, 0);

SELECT 'parts before dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_cleanup' AND active ORDER BY name;

SELECT 'data before dry run';
SELECT * FROM t_dry_run_cleanup ORDER BY uid, version;

OPTIMIZE TABLE t_dry_run_cleanup DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0' CLEANUP;

-- After DRY RUN, parts must remain unchanged: no merge committed.
SELECT 'parts after dry run with CLEANUP';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_cleanup' AND active ORDER BY name;

SELECT 'data after dry run with CLEANUP';
SELECT * FROM t_dry_run_cleanup ORDER BY uid, version;

SYSTEM FLUSH LOGS query_log;
SELECT 'profile events CLEANUP';

SELECT
    query,
    query_kind,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergeSourceParts'],
    ProfileEvents['MergeWrittenRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'OPTIMIZE TABLE t_dry_run_cleanup DRY RUN PARTS%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_dry_run_cleanup;
