#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-cpu-aarch64, no-random-settings
# Some settings can be different for builds with sanitizers or aarch64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Note that this is a broad check. A per version check is done in the upgrade test
# Baseline generated with 24.7.2
# clickhouse local --query "select name, default from system.settings order by name format TSV" > 02995_baseline_24_7_2.tsv
$CLICKHOUSE_LOCAL --query "
    WITH old_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_baseline_24_7_2.tsv', 'TSV', 'name String, default String')
    ),
    new_settings AS
    (
        -- Ignore settings that depend on the machine config (max_threads and similar)
        SELECT name, default FROM system.settings WHERE default NOT LIKE '%auto(%'
    )
    SELECT * FROM
    (
        SELECT 'PLEASE ADD THE NEW SETTING TO SettingsChangesHistory.cpp: ' || name || ' WAS ADDED',
        FROM new_settings
        WHERE (name NOT IN (
            SELECT name
            FROM old_settings
        )) AND (name NOT IN (
            SELECT arrayJoin(tupleElement(changes, 'name'))
            FROM system.settings_changes
            WHERE splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 7
        ))
        UNION ALL
        (
            SELECT 'PLEASE ADD THE SETTING VALUE CHANGE TO SettingsChangesHistory.cpp: ' || name || ' WAS CHANGED FROM ' || old_settings.default || ' TO ' || new_settings.default,
            FROM new_settings
            LEFT JOIN old_settings ON new_settings.name = old_settings.name
            WHERE (new_settings.default != old_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE splitByChar('.', version)[1]::UInt64 >= 24 AND splitByChar('.', version)[2]::UInt64 > 7
            ))
        )
    )
"
