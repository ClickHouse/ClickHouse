#!/usr/bin/env bash
# Tags: no-cpu-aarch64, no-random-settings, no-random-merge-tree-settings
# Some settings can be different for builds with sanitizers or aarch64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Note that this is a broad check. A per version check is done in the upgrade test
# Baselines generated with 24.11.2
# clickhouse local --query "select name, default from system.settings order by name format TSV" > 02995_settings_24_11_2.tsv
# clickhouse local --query "select name, value from system.merge_tree_settings order by name format TSV" > 02995_merge_tree_settings_settings_24_11_2.tsv
$CLICKHOUSE_LOCAL --query "
    WITH old_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_settings_24_11_2.tsv', 'TSV', 'name String, default String')
    ),
    old_merge_tree_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_merge_tree_settings_settings_24_11_2.tsv', 'TSV', 'name String, default String')
    ),
    new_settings AS
    (
        -- Ignore settings that depend on the machine config (max_threads and similar)
        SELECT name, default FROM system.settings WHERE default NOT LIKE '%auto(%'
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
            WHERE type = 'Core' AND splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 11
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
                WHERE type = 'MergeTree' AND splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 11
            ))
        )
        UNION ALL
        (
            SELECT 'PLEASE ADD THE SETTING VALUE CHANGE TO SettingsChangesHistory.cpp: ' || name || ' WAS CHANGED FROM ' || old_settings.default || ' TO ' || new_settings.default
            FROM new_settings
            LEFT JOIN old_settings ON new_settings.name = old_settings.name
            WHERE (new_settings.default != old_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE type = 'Core' AND splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 11
            )) AND (
                -- Different values for sanitizers
                ( SELECT count() FROM system.build_options WHERE name = 'CXX_FLAGS' AND position('sanitize' IN value) = 1 )
                AND
                name NOT IN ('query_profiler_cpu_time_period_ns', 'query_profiler_real_time_period_ns')
            )
        )
        UNION ALL
        (
            SELECT 'PLEASE ADD THE MERGE_TREE_SETTING VALUE CHANGE TO SettingsChangesHistory.cpp: ' || name || ' WAS CHANGED FROM ' || old_merge_tree_settings.default || ' TO ' || new_merge_tree_settings.default
            FROM new_merge_tree_settings
            LEFT JOIN old_merge_tree_settings ON new_merge_tree_settings.name = old_merge_tree_settings.name
            WHERE (new_merge_tree_settings.default != old_merge_tree_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE type = 'MergeTree' AND splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 11
            ))
        )
    )
"
