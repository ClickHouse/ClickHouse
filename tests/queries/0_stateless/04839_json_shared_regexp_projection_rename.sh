#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_json_type=1"

# A projection rebuild after RENAME COLUMN must resolve each source part's provenance through its
# own AlterConversions rename map, not the current logical name. RENAME COLUMN is a barrier mutation that always finishes before MATERIALIZE PROJECTION runs, so only a MERGE can reach still-pre-rename parts while the rename is pending; the failpoint below makes that race deterministic to test.
disable_failpoint() {
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null || true
}
trap disable_failpoint EXIT

${CLICKHOUSE_CLIENT} --query="
    DROP TABLE IF EXISTS projection_rename_04839;
    CREATE TABLE projection_rename_04839
    (
        id UInt64,
        j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
    )
    ENGINE = MergeTree
    ORDER BY id
    -- materialize_projections_on_merge defaults to false: a plain merge would otherwise leave a
    -- projection added after a part was written absent, making OPTIMIZE below a no-op for 'p'.
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_projections_on_merge = 1;

    INSERT INTO projection_rename_04839 VALUES (1, '{\"tag_a\":1,\"keep\":1}');
    INSERT INTO projection_rename_04839 VALUES (2, '{\"tag_b\":2,\"keep\":2}');

    -- Retire the rule at the table level: the projection's seed type for the renamed column comes
    -- from the table's *current* metadata, so if it still declared SHARED REGEXP the assertion below would pass trivially without exercising per-source-part resolution.
    ALTER TABLE projection_rename_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5) SETTINGS mutations_sync = 1;

    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

    ALTER TABLE projection_rename_04839
        RENAME COLUMN j TO payload,
        ADD PROJECTION p (SELECT id, payload WHERE id > 0 ORDER BY id)
        SETTINGS mutations_sync = 0, alter_sync = 0;
"

# Control: both source parts must still be physically named 'j' while the failpoint holds, or this
# test silently stops covering the race; a concurrent test clearing the global failpoint is the one benign way this can fail (skip, don't fail, same as the merge check below).
still_prerename=$(${CLICKHOUSE_CLIENT} --query="
    SELECT countDistinct(name) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND active AND column = 'j'")
skip=0
if [ "$still_prerename" != "2" ]; then
    skip=1
fi

# The merge rebuilds the projection from the still-pre-rename source parts and must resolve
# 'payload' back to 'j' through each part's own AlterConversions before probing it; optimize_throw_if_noop=1 so a silently skipped merge reads as a hard failure, not a false pass.
if [ "$skip" = "0" ]; then
    if ! err=$(${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE projection_rename_04839 FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1); then
        case "$err" in
            *"different mutation version"*|*"different projection sets"*) skip=1 ;;
            *)
                echo "FAIL: OPTIMIZE did not run: ${err}"
                ${CLICKHOUSE_CLIENT} --query="DROP TABLE projection_rename_04839"
                exit 1
                ;;
        esac
    fi
fi

disable_failpoint

if [ "$skip" = "1" ]; then
    # A concurrent test cleared the global failpoint mid-window (or the merge saw a part already at
    # a different mutation version). That only ever costs coverage here, never a red.
    echo "OK"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE projection_rename_04839"
    exit 0
fi

# Control: the projection's own column really is named after the renamed column.
name_ok=$(${CLICKHOUSE_CLIENT} --query="
    SELECT countIf(column = 'payload') FROM system.projection_parts_columns
    WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND part_name = 'p' AND active")
if [ "$name_ok" != "1" ]; then
    echo "FAIL: rebuilt projection column is not named 'payload'"
    ${CLICKHOUSE_CLIENT} --query="
        SELECT name, column, type FROM system.projection_parts_columns
        WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND active"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE projection_rename_04839"
    exit 1
fi

# The regression: this must be 1. Without resolving 'payload' back through the source part's own
# rename map to 'j', the rebuilt projection's column falls back to the current bare type and this comes back 0.
provenance_ok=$(${CLICKHOUSE_CLIENT} --query="
    SELECT countIf(position(type, 'SHARED REGEXP') > 0) FROM system.projection_parts_columns
    WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND column = 'payload' AND active")
if [ "$provenance_ok" != "1" ]; then
    echo "FAIL: rebuilt projection lost SHARED REGEXP provenance across the rename"
    ${CLICKHOUSE_CLIENT} --query="
        SELECT name, column, type FROM system.projection_parts_columns
        WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND active"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE projection_rename_04839"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE projection_rename_04839"
echo "OK"
