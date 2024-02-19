#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-cpu-aarch64
# Some settings can be different for builds with sanitizers

# Note that this is a broad check. A per version check is done in the upgrade test
# Baseline generated with 23.12
# clickhouse local --query "select name, default from system.settings order by name format TSV" > 02995_baseline_23_12_1.tsv
$CLICKHOUSE_LOCAL --query "
    WITH old_settings AS
    (
        SELECT * FROM file('${CUR_DIR}/02995_baseline_23_12_1.tsv', 'TSV', 'name String, default String')
    ),
    new_settings AS
    (
        select name, default from system.settings order by name
    )
    SELECT * FROM
    (
        SELECT 'PLEASE ADD THE NEW SETTING TO SettingsChangesHistory.h: ' || name || ' WAS ADDED',
        FROM new_settings
        WHERE (name NOT IN (
            SELECT name
            FROM old_settings
        )) AND (name NOT IN (
            SELECT arrayJoin(tupleElement(changes, 'name'))
            FROM system.settings_changes
            WHERE splitByChar('.', version())[1] >= '24'
        ))
        UNION ALL
        (
            SELECT 'PLEASE ADD THE SETTING VALUE CHANGE TO SettingsChangesHistory.h: ' || name || ' WAS CHANGED FROM ' || old_settings.default || ' TO ' || new_settings.default,
            FROM new_settings
            LEFT JOIN old_settings ON new_settings.name = old_settings.name
            WHERE (new_settings.default != old_settings.default) AND (name NOT IN (
                SELECT arrayJoin(tupleElement(changes, 'name'))
                FROM system.settings_changes
                WHERE splitByChar('.', version())[1] >= '24'
            ))
        )
    )
"
