-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_merge_profile_events_1;

CREATE TABLE t_merge_profile_events_1 (id UInt64, v1 UInt64, v2 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_merge_profile_events_1 SELECT number, number, number FROM numbers(10000);
INSERT INTO t_merge_profile_events_1 SELECT number, number, number FROM numbers(10000);

OPTIMIZE TABLE t_merge_profile_events_1 FINAL;
SYSTEM FLUSH LOGS;

SELECT
    merge_algorithm,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergedColumns'],
    ProfileEvents['GatheredColumns'],
    ProfileEvents['MergedUncompressedBytes'],
    ProfileEvents['MergeTotalMilliseconds'] > 0,
    ProfileEvents['MergeExecuteMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageExecuteMilliseconds'] > 0
FROM system.part_log WHERE database = currentDatabase() AND table = 't_merge_profile_events_1' AND event_type = 'MergeParts' AND part_name = 'all_1_2_1';

DROP TABLE IF EXISTS t_merge_profile_events_1;

DROP TABLE IF EXISTS t_merge_profile_events_2;

CREATE TABLE t_merge_profile_events_2 (id UInt64, v1 UInt64, v2 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_merge_profile_events_2 SELECT number, number, number FROM numbers(10000);
INSERT INTO t_merge_profile_events_2 SELECT number, number, number FROM numbers(10000);

OPTIMIZE TABLE t_merge_profile_events_2 FINAL;
SYSTEM FLUSH LOGS;

SELECT
    merge_algorithm,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergedColumns'],
    ProfileEvents['GatheredColumns'],
    ProfileEvents['MergedUncompressedBytes'],
    ProfileEvents['MergeTotalMilliseconds'] > 0,
    ProfileEvents['MergeExecuteMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageExecuteMilliseconds'] > 0,
    ProfileEvents['MergeVerticalStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeVerticalStageExecuteMilliseconds'] > 0,
FROM system.part_log WHERE database = currentDatabase() AND table = 't_merge_profile_events_2' AND event_type = 'MergeParts' AND part_name = 'all_1_2_1';

DROP TABLE IF EXISTS t_merge_profile_events_2;

DROP TABLE IF EXISTS t_merge_profile_events_3;

CREATE TABLE t_merge_profile_events_3 (id UInt64, v1 UInt64, v2 UInt64, PROJECTION p (SELECT v2, v2 * v2, v2 * 2, v2 * 10, v1 ORDER BY v1))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_merge_profile_events_3 SELECT number, number, number FROM numbers(100000);
INSERT INTO t_merge_profile_events_3 SELECT number, number, number FROM numbers(100000);

OPTIMIZE TABLE t_merge_profile_events_3 FINAL;
SYSTEM FLUSH LOGS;

SELECT
    merge_algorithm,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergedColumns'],
    ProfileEvents['GatheredColumns'],
    ProfileEvents['MergedUncompressedBytes'],
    ProfileEvents['MergeTotalMilliseconds'] > 0,
    ProfileEvents['MergeExecuteMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeHorizontalStageExecuteMilliseconds'] > 0,
    ProfileEvents['MergeVerticalStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeVerticalStageExecuteMilliseconds'] > 0,
    ProfileEvents['MergeProjectionStageTotalMilliseconds'] > 0,
    ProfileEvents['MergeProjectionStageExecuteMilliseconds'] > 0,
    ProfileEvents['MergeExecuteMilliseconds'] <= duration_ms,
    ProfileEvents['MergeTotalMilliseconds'] <= duration_ms
FROM system.part_log WHERE database = currentDatabase() AND table = 't_merge_profile_events_3' AND event_type = 'MergeParts' AND part_name = 'all_1_2_1';

DROP TABLE IF EXISTS t_merge_profile_events_3;
