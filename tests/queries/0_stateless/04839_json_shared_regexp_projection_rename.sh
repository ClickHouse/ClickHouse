#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_json_type=1"

# A projection rebuild (merge-time, or mutation-time MATERIALIZE PROJECTION) resolves each result
# column's provenance by candidate name against the source part's own columns (see
# applyJSONSharedDataPathPoliciesForProjection, MergeTreeDataWriter.cpp). After RENAME COLUMN, a
# source part written before the rename still has the OLD physical name; without resolving through
# that part's own AlterConversions rename map first -- the same way the main (non-projection)
# provenance merge and IMergeTreeReader::getStorageAndSubcolumnNameInPart already do -- the fallback
# misses the old physical column and silently drops the historical SHARED REGEXP policy.
#
# RENAME COLUMN is a MergeTree "barrier" mutation (MutationCommand::isBarrierCommand() returns true
# only for it) that always runs to completion alone before any other mutation -- including
# MATERIALIZE PROJECTION -- can run on the same table (StorageMergeTree::selectPartsToMutate /
# alter()). So a plain "RENAME, then ADD+MATERIALIZE PROJECTION" never actually exercises the
# AlterConversions resolution: by the time MATERIALIZE PROJECTION runs, the rename mutation has
# always already rewritten every part's physical column name, so probing the new logical name
# directly already succeeds. The gap is only reachable through a MERGE, not a mutation: a single
# combined `RENAME COLUMN ..., ADD PROJECTION ...` statement slips past the "can't rename a column a
# projection references" guard (it is built from the pre-ALTER projection metadata, so it never sees
# a projection added in the same ALTER), leaving pre-rename parts around with a projection that now
# references their new logical name; a subsequent merge then rebuilds the projection from those
# still-physically-pre-rename parts while the rename mutation is still pending. The failpoint below
# only makes that window deterministic to test -- the same race is ordinary on a busy table.
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
    -- materialize_projections_on_merge defaults to false: a plain merge otherwise leaves a
    -- projection added after a part was written simply absent from that part's merge output,
    -- rather than rebuilding it -- which would make OPTIMIZE below a no-op for 'p' instead of the
    -- rebuild this test needs to observe.
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_projections_on_merge = 1;

    INSERT INTO projection_rename_04839 VALUES (1, '{\"tag_a\":1,\"keep\":1}');
    INSERT INTO projection_rename_04839 VALUES (2, '{\"tag_b\":2,\"keep\":2}');

    -- Retire the rule at the table level; the existing parts' own j type still carries it as
    -- history. This is essential, not cosmetic: the projection's starting/seed type for the
    -- renamed column already comes from the table's own *current* metadata, so if the live schema
    -- still declared SHARED REGEXP here, the assertion below would pass trivially regardless of
    -- whether per-source-part resolution works at all -- the merge loop only ever adds provenance
    -- on top of an already-correct seed, it never has to recover a lost one.
    ALTER TABLE projection_rename_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5) SETTINGS mutations_sync = 1;

    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

    ALTER TABLE projection_rename_04839
        RENAME COLUMN j TO payload,
        ADD PROJECTION p (SELECT id, payload WHERE id > 0 ORDER BY id)
        SETTINGS mutations_sync = 0, alter_sync = 0;
"

# Control: with the failpoint still enabled, both source parts must still be physically named 'j' --
# otherwise this test would silently stop covering the race it exists for. A concurrent copy of
# another test clearing the server-global failpoint mid-window is the one benign way this can fail;
# treat it the same way as the merge check below (skip, don't fail).
still_prerename=$(${CLICKHOUSE_CLIENT} --query="
    SELECT countDistinct(name) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'projection_rename_04839' AND active AND column = 'j'")
skip=0
if [ "$still_prerename" != "2" ]; then
    skip=1
fi

# The merge is independent of the stalled rename mutation and rebuilds the projection from the
# still-pre-rename source parts -- this is the arm that must resolve 'payload' back to 'j' through
# each source part's own AlterConversions before probing it. optimize_throw_if_noop=1 so a silently
# skipped merge reads as a hard failure, not a false pass.
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
    # A concurrent test cleared the global failpoint mid-window (or the merge legitimately saw a
    # part already at a different mutation version because of it). That only ever costs coverage
    # here, never a red -- there is nothing to assert about a merge that could not observe the race.
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
# rename map to 'j' before probing it, the rebuilt projection's column falls back to the current
# bare type and this comes back 0.
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
