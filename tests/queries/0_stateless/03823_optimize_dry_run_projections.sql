DROP TABLE IF EXISTS t_dry_run_proj;

CREATE TABLE t_dry_run_proj (key UInt64, value UInt64, PROJECTION p_sum (SELECT key, sum(value) GROUP BY key))
ENGINE = MergeTree ORDER BY key;

SYSTEM STOP MERGES t_dry_run_proj;

INSERT INTO t_dry_run_proj VALUES (1, 10), (1, 20);
INSERT INTO t_dry_run_proj VALUES (2, 30), (3, 40);
INSERT INTO t_dry_run_proj VALUES (2, 50), (4, 60);

SELECT 'parts before dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_proj' AND active ORDER BY name;

SELECT 'projection parts before dry run';
SELECT parent_name, name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_dry_run_proj' AND active ORDER BY parent_name, name;

OPTIMIZE TABLE t_dry_run_proj DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0';

-- After DRY RUN, parts must remain unchanged: no merge committed.
SELECT 'parts after dry run with projections';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_proj' AND active ORDER BY name;

SELECT 'projection parts after dry run';
SELECT parent_name, name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_dry_run_proj' AND active ORDER BY parent_name, name;

SYSTEM FLUSH LOGS query_log;
SELECT 'profile events projections';

SELECT
    query,
    query_kind,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergeSourceParts'],
    ProfileEvents['MergeWrittenRows']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND query LIKE 'OPTIMIZE TABLE t_dry_run_proj DRY RUN PARTS%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_dry_run_proj;

-- OPTIMIZE DRY RUN with enable_block_number_column: the merged projection sub-part must not
-- gain a _block_number column (the projection metadata does not declare it), otherwise
-- checkDataPart rejects it as CORRUPTED_DATA.
DROP TABLE IF EXISTS t_dry_run_proj_bn;

CREATE TABLE t_dry_run_proj_bn (key UInt64, value UInt64, PROJECTION p_sum (SELECT key, sum(value) GROUP BY key))
ENGINE = MergeTree ORDER BY key SETTINGS enable_block_number_column = 1;

SYSTEM STOP MERGES t_dry_run_proj_bn;

INSERT INTO t_dry_run_proj_bn VALUES (1, 10), (1, 20);
INSERT INTO t_dry_run_proj_bn VALUES (2, 30), (3, 40);
INSERT INTO t_dry_run_proj_bn VALUES (2, 50), (4, 60);

SELECT 'dry run with block number column';
OPTIMIZE TABLE t_dry_run_proj_bn DRY RUN PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0';

-- A real merge must produce a projection part whose columns match the projection metadata.
SYSTEM START MERGES t_dry_run_proj_bn;
OPTIMIZE TABLE t_dry_run_proj_bn FINAL;

SELECT 'projection part columns after merge with block number column';
SELECT DISTINCT column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_dry_run_proj_bn' AND active
ORDER BY column;

SELECT 'check table with block number column';
CHECK TABLE t_dry_run_proj_bn SETTINGS check_query_single_value_result = 1;

DROP TABLE t_dry_run_proj_bn;
