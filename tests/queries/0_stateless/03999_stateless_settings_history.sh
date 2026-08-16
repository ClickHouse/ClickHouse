#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
#
# Stateless replacement for 02995_new_settings_history:
#   * No per-release regeneration and no hard-coded version threshold.
#   * The only committed data is a FROZEN snapshot of the settings (name, kind and
#     default value) that existed when this test was introduced
#     (03999_settings_history_baseline.tsv). It is never regenerated:
#       - any setting NOT present in it was added afterwards and must be recorded in
#         `SettingsChangesHistory.cpp` (surfaced via `system.settings_changes`);
#       - any setting present in it whose default differs from the snapshot must be
#         recorded there too (otherwise a default change to a long-lived setting that
#         has no history row would go unnoticed).
#     so that the `compatibility` setting keeps working.
#   * Aliases and obsolete settings are ignored.
#
# The snapshot only ever needs editing to REMOVE a name (when an old setting is
# dropped); new settings must never be added to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASELINE="${CUR_DIR}/03999_settings_history_baseline.tsv"
# Settings whose current default already disagrees with the newest new_value recorded in
# SettingsChangesHistory.cpp (i.e. a default change that predates this test and was never
# recorded). This list may only SHRINK: each name should be fixed in the history and removed.
VALUE_DRIFT_IGNORE="${CUR_DIR}/03999_settings_value_drift_ignore.txt"

$CLICKHOUSE_LOCAL --query "
    WITH
        baseline AS
        (
            SELECT name, kind, default FROM file('${BASELINE}', 'TSV', 'name String, kind String, default String')
        ),
        value_drift_ignore AS
        (
            SELECT name FROM file('${VALUE_DRIFT_IGNORE}', 'LineAsString', 'name String')
        ),
        -- Settings whose compiled default differs in Cloud. The frozen baseline holds the OSS
        -- value and these have no history row, so on a Cloud build their default legitimately
        -- differs from the baseline; exclude them from the value comparison there (as 02995 did).
        -- On OSS this list is empty, so they are checked normally.
        cloud_divergent_settings AS
        (
            SELECT arrayJoin(if(
                (SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD') = '1',
                ['max_table_size_to_drop', 'max_partition_size_to_drop', 'min_bytes_for_wide_part'],
                emptyArrayString()
            )) AS name
        ),
        -- The sampling query profiler is unavailable under MemorySanitizer because its signal
        -- handler can interrupt the sanitizer while it is reporting an error. Its defaults are
        -- therefore zero only in that build; this is a build capability difference, not a
        -- compatibility change. Detect the build capability from the compiler flags rather
        -- than from the defaults being checked below.
        memory_sanitizer_divergent_settings AS
        (
            SELECT arrayJoin(if(
                (SELECT position('sanitize=memory' IN value) > 0 FROM system.build_options WHERE name = 'CXX_FLAGS'),
                ['query_profiler_real_time_period_ns', 'query_profiler_cpu_time_period_ns'],
                emptyArrayString()
            )) AS name
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
        -- For every documented setting, the value the current default is expected to equal is
        -- the new_value of the LAST entry applied by the compatibility mechanism: entries are
        -- applied in ascending version order and, within a version, in vector order (the order
        -- they appear in the block). A setting may legitimately appear more than once in the
        -- same block, so ties on version are broken by the entry index - argMax over the tuple
        -- (version, index) reproduces the effective 'latest' value deterministically. If it
        -- differs from the current default, a default was changed in code without recording it
        -- in SettingsChangesHistory.cpp, so the compatibility setting restores a wrong value.
        session_expected_default AS
        (
            SELECT name, argMax(new_value, (vnum, idx)) AS expected
            FROM
            (
                SELECT c.1 AS name, c.3 AS new_value,
                    splitByChar('.', version)[1]::UInt64 * 1000 + splitByChar('.', version)[2]::UInt64 AS vnum,
                    idx
                FROM system.settings_changes
                ARRAY JOIN changes AS c, arrayEnumerate(changes) AS idx
                WHERE type = 'Session'
            )
            GROUP BY name
        ),
        mergetree_expected_default AS
        (
            SELECT name, argMax(new_value, (vnum, idx)) AS expected
            FROM
            (
                SELECT c.1 AS name, c.3 AS new_value,
                    splitByChar('.', version)[1]::UInt64 * 1000 + splitByChar('.', version)[2]::UInt64 AS vnum,
                    idx
                FROM system.settings_changes
                ARRAY JOIN changes AS c, arrayEnumerate(changes) AS idx
                WHERE type = 'MergeTree'
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

        -- Session default changed in code but the newest recorded new_value was not updated to match.
        SELECT 'PLEASE RECORD THE DEFAULT CHANGE IN SettingsChangesHistory.cpp: ' || s.name
            || ' default is ' || s.default || ' but history last records ' || e.expected
        FROM system.settings s
        JOIN session_expected_default e ON s.name = e.name
        WHERE s.is_obsolete = 0 AND s.alias_for = ''
          AND s.default NOT LIKE 'auto(%'   -- runtime-derived value, not comparable
          AND s.type != 'Map'               -- avoid '{}' vs '' rendering differences
          AND s.name NOT IN (SELECT name FROM value_drift_ignore)
          AND s.name NOT IN (SELECT name FROM cloud_divergent_settings)
          AND s.name NOT IN (SELECT name FROM memory_sanitizer_divergent_settings)
          AND if(s.type = 'Bool',
                 -- history may store true/false, system.settings shows 1/0
                 transform(e.expected, ['true', 'false'], ['1', '0'], e.expected) != s.default,
              if(s.type = 'Float',
                 -- Float settings render at different precision (Float32 value shown via Float64)
                 toFloat32OrNull(e.expected) != toFloat32OrNull(s.default),
                 e.expected != s.default))

        UNION ALL

        -- MergeTree default changed in code but the newest recorded new_value was not updated to match.
        SELECT 'PLEASE RECORD THE MERGE_TREE_SETTING DEFAULT CHANGE IN SettingsChangesHistory.cpp: ' || s.name
            || ' default is ' || s.default || ' but history last records ' || e.expected
        FROM system.merge_tree_settings s
        JOIN mergetree_expected_default e ON s.name = e.name
        WHERE s.is_obsolete = 0
          AND s.default NOT LIKE 'auto(%'
          AND s.type != 'Map'
          AND s.name NOT IN (SELECT name FROM value_drift_ignore)
          AND s.name NOT IN (SELECT name FROM cloud_divergent_settings)
          AND s.name NOT IN (SELECT name FROM memory_sanitizer_divergent_settings)
          AND if(s.type = 'Bool',
                 transform(e.expected, ['true', 'false'], ['1', '0'], e.expected) != s.default,
              if(s.type = 'Float',
                 toFloat32OrNull(e.expected) != toFloat32OrNull(s.default),
                 e.expected != s.default))

        UNION ALL

        -- A long-lived Session setting present in the frozen baseline whose default no longer
        -- matches the baseline but that has NO history row at all: the value-drift arm above
        -- joins on the history and so cannot see it. Compare against the frozen baseline default
        -- (both come from system.settings, so the representation matches - no normalization). Such
        -- a change must be recorded in SettingsChangesHistory.cpp so the compatibility setting restores it.
        SELECT 'PLEASE RECORD THE DEFAULT CHANGE IN SettingsChangesHistory.cpp: ' || s.name
            || ' default changed from ' || b.default || ' to ' || s.default || ' since the baseline but has no history entry'
        FROM system.settings s
        JOIN baseline b ON b.name = s.name AND b.kind = 'Session'
        WHERE s.is_obsolete = 0 AND s.alias_for = ''
          AND s.default NOT LIKE 'auto(%'   -- runtime-derived value, machine-specific
          AND s.default != b.default
          AND s.name NOT IN (SELECT name FROM session_documented)
          AND s.name NOT IN (SELECT name FROM value_drift_ignore)
          AND s.name NOT IN (SELECT name FROM cloud_divergent_settings)
          AND s.name NOT IN (SELECT name FROM memory_sanitizer_divergent_settings)

        UNION ALL

        -- Same guard for MergeTree settings without a history row.
        SELECT 'PLEASE RECORD THE MERGE_TREE_SETTING DEFAULT CHANGE IN SettingsChangesHistory.cpp: ' || s.name
            || ' default changed from ' || b.default || ' to ' || s.default || ' since the baseline but has no history entry'
        FROM system.merge_tree_settings s
        JOIN baseline b ON b.name = s.name AND b.kind = 'MergeTree'
        WHERE s.is_obsolete = 0
          AND s.default NOT LIKE 'auto(%'
          AND s.default != b.default
          AND s.name NOT IN (SELECT name FROM mergetree_documented)
          AND s.name NOT IN (SELECT name FROM value_drift_ignore)
          AND s.name NOT IN (SELECT name FROM cloud_divergent_settings)
    )
    ORDER BY message
"
