#!/usr/bin/env bash
# Tags: no-cpu-aarch64, no-random-settings, no-random-merge-tree-settings
# Some settings can be different for builds with aarch64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Ignore settings that, for historic reasons, have different values in Cloud
IGNORED_SETTINGS_FOR_CLOUD="1 = 1"
IGNORED_MERGETREE_SETTINGS_FOR_CLOUD="1 = 1"
if [[ $($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'") -eq 1 ]];
then
  IGNORED_SETTINGS_FOR_CLOUD="name NOT IN ('max_table_size_to_drop', 'max_partition_size_to_drop')"
  IGNORED_MERGETREE_SETTINGS_FOR_CLOUD="name NOT IN ('allow_remote_fs_zero_copy_replication', 'min_bytes_for_wide_part')"
fi

IGNORE_SETTINGS_FOR_SANITIZERS="1=1"
if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() != 0 FROM system.build_options WHERE name = 'CXX_FLAGS' AND position('sanitize' IN value) != 0") -eq 1 ]];
then
  IGNORE_SETTINGS_FOR_SANITIZERS="name NOT IN ('query_profiler_cpu_time_period_ns', 'query_profiler_real_time_period_ns')"
fi

# Note that this is a broad check. A per version check is done in the upgrade test
# Baselines generated with v25.2.3.14 (pre-release)
# clickhouse local --query "select name, default from system.settings order by name format TSV" > 02995_settings_25_6_1.tsv
# clickhouse local --query "select name, value from system.merge_tree_settings order by name format TSV" > 02995_merge_tree_settings_settings_25_6_1.tsv
$CLICKHOUSE_LOCAL --query "
    WITH old_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_settings_25_6_1.tsv', 'TSV', 'name String, default String')
    ),
    old_merge_tree_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_merge_tree_settings_settings_25_6_1.tsv', 'TSV', 'name String, default String')
    ),
    new_settings AS
    (
        SELECT name, default FROM system.settings WHERE default NOT LIKE '%auto(%' AND ${IGNORED_SETTINGS_FOR_CLOUD}
    ),
    new_merge_tree_settings AS
    (
        SELECT name, value as default FROM system.merge_tree_settings WHERE default NOT LIKE '%auto(%'
    )
    SELECT * FROM
    (
        SELECT 'PLEASE ADD THE NEW SETTING TO SettingsChangesHistory.cpp: ' || name || ' WAS ADDED'
        FROM new_settings
        WHERE (name NOT IN (
            SELECT name
            FROM old_settings
        )) AND (name NOT IN (
            SELECT arrayJoin(tupleElement(changes, 'name'))
            FROM system.settings_changes
            WHERE type = 'Session' AND splitByChar('.', version)[1]::UInt64 > 25 OR (splitByChar('.', version)[1]::UInt64 == 25 AND splitByChar('.', version)[2]::UInt64 > 6)
        ))
        UNION ALL
        (
            SELECT 'PLEASE ADD THE NEW MERGE_TREE_SETTING TO SettingsChangesHistory.cpp: ' || name || ' WAS ADDED'
            FROM new_merge_tree_settings
            WHERE (name NOT IN (
                SELECT name
                FROM old_merge_tree_settings
            )) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE type = 'MergeTree' AND splitByChar('.', version)[1]::UInt64 > 25 OR (splitByChar('.', version)[1]::UInt64 == 25 AND splitByChar('.', version)[2]::UInt64 > 6)
            ))
        )
        UNION ALL
        (
            SELECT 'PLEASE ADD THE SETTING VALUE CHANGE TO SettingsChangesHistory.cpp: ' || name || ' WAS CHANGED FROM ' || old_settings.default || ' TO ' || new_settings.default
            FROM new_settings
            JOIN old_settings ON new_settings.name = old_settings.name
            WHERE (new_settings.default != old_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE type = 'Session' AND splitByChar('.', version)[1]::UInt64 > 25 OR (splitByChar('.', version)[1]::UInt64 == 25 AND splitByChar('.', version)[2]::UInt64 > 6)
            )) AND ${IGNORE_SETTINGS_FOR_SANITIZERS}
        )
        UNION ALL
        (
            SELECT 'PLEASE ADD THE MERGE_TREE_SETTING VALUE CHANGE TO SettingsChangesHistory.cpp: ' || name || ' WAS CHANGED FROM ' || old_merge_tree_settings.default || ' TO ' || new_merge_tree_settings.default
            FROM new_merge_tree_settings
            JOIN old_merge_tree_settings ON new_merge_tree_settings.name = old_merge_tree_settings.name
            WHERE (new_merge_tree_settings.default != old_merge_tree_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE type = 'MergeTree' AND splitByChar('.', version)[1]::UInt64 > 25 OR (splitByChar('.', version)[1]::UInt64 == 25 AND splitByChar('.', version)[2]::UInt64 > 6)
            )) AND ${IGNORED_MERGETREE_SETTINGS_FOR_CLOUD}
        )
    )
"
