#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
#
# Stateless replacement for 02995_new_settings_history:
#   * No per-release regeneration and no hard-coded version threshold.
#   * The only committed data is a FROZEN, name-only snapshot of the settings
#     that existed when this test was introduced (03999_settings_history_baseline_names.tsv).
#     It is never regenerated: any setting NOT present in it was added afterwards
#     and therefore must be recorded in `SettingsChangesHistory.cpp` (surfaced via
#     `system.settings_changes`) so that the `compatibility` setting keeps working.
#   * Aliases and obsolete settings are ignored.
#
# The snapshot only ever needs editing to REMOVE a name (when an old setting is
# dropped); new settings must never be added to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASELINE="${CUR_DIR}/03999_settings_history_baseline_names.tsv"
# Settings whose current default already disagrees with the newest new_value recorded in
# SettingsChangesHistory.cpp (i.e. a default change that predates this test and was never
# recorded). This list may only SHRINK: each name should be fixed in the history and removed.
VALUE_DRIFT_IGNORE="${CUR_DIR}/03999_settings_value_drift_ignore.txt"

$CLICKHOUSE_LOCAL --query "
    WITH
        baseline AS
        (
            SELECT name, kind FROM file('${BASELINE}', 'TSV', 'name String, kind String')
        ),
        value_drift_ignore AS
        (
            SELECT name FROM file('${VALUE_DRIFT_IGNORE}', 'LineAsString', 'name String')
        ),
        session_documented AS
        (
            SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name
            FROM system.settings_changes WHERE type = 'Session'
        ),
        mergetree_documented AS
        (
            SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name
            FROM system.settings_changes WHERE type = 'MergeTree'
        ),
        -- For every documented setting, the new_value recorded at its HIGHEST version is what
        -- the current default is expected to be. If they differ, a default was changed in code
        -- without recording it in SettingsChangesHistory.cpp, so the compatibility setting restores a wrong value.
        session_expected_default AS
        (
            SELECT name, argMax(new_value, vnum) AS expected
            FROM
            (
                SELECT c.1 AS name, c.3 AS new_value,
                    splitByChar('.', version)[1]::UInt64 * 1000 + splitByChar('.', version)[2]::UInt64 AS vnum
                FROM system.settings_changes ARRAY JOIN changes AS c WHERE type = 'Session'
            )
            GROUP BY name
        )
    SELECT * FROM
    (
        -- New session setting that is neither in the frozen snapshot nor documented.
        SELECT 'PLEASE ADD THE NEW SETTING TO SettingsChangesHistory.cpp: ' || name AS message
        FROM system.settings
        WHERE alias_for = '' AND is_obsolete = 0
          AND name NOT IN (SELECT name FROM baseline WHERE kind = 'Session')
          AND name NOT IN (SELECT name FROM session_documented)

        UNION ALL

        -- New MergeTree setting that is neither in the frozen snapshot nor documented.
        SELECT 'PLEASE ADD THE NEW MERGE_TREE_SETTING TO SettingsChangesHistory.cpp: ' || name
        FROM system.merge_tree_settings
        WHERE is_obsolete = 0
          AND name NOT IN (SELECT name FROM baseline WHERE kind = 'MergeTree')
          AND name NOT IN (SELECT name FROM mergetree_documented)

        UNION ALL

        -- Dangling history entry: a documented name that no longer exists as a setting
        -- (typo or a rename that forgot to keep the old name). Catches the reverse mistake.
        SELECT 'SETTING IN SettingsChangesHistory.cpp DOES NOT EXIST (typo/rename?): ' || name
        FROM session_documented
        WHERE name NOT IN (SELECT name FROM system.settings)

        UNION ALL

        SELECT 'MERGE_TREE_SETTING IN SettingsChangesHistory.cpp DOES NOT EXIST (typo/rename?): ' || name
        FROM mergetree_documented
        WHERE name NOT IN (SELECT name FROM system.merge_tree_settings)

        UNION ALL

        -- Default changed in code but the newest recorded new_value was not updated to match.
        SELECT 'PLEASE RECORD THE DEFAULT CHANGE IN SettingsChangesHistory.cpp: ' || s.name
            || ' default is ' || s.default || ' but history last records ' || e.expected
        FROM system.settings s
        JOIN session_expected_default e ON s.name = e.name
        WHERE s.is_obsolete = 0 AND s.alias_for = ''
          AND s.default NOT LIKE 'auto(%'   -- runtime-derived value, not comparable
          AND s.type != 'Map'               -- avoid '{}' vs '' rendering differences
          AND s.name NOT IN (SELECT name FROM value_drift_ignore)
          -- normalize Bool rendering (history may store true/false, system.settings shows 1/0)
          AND if(s.type = 'Bool', transform(e.expected, ['true', 'false'], ['1', '0'], e.expected), e.expected) != s.default
    )
    ORDER BY message
"
