#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest

# Regression test for: https://github.com/ClickHouse/ClickHouse/issues/102153
#
# Scenario: a part is fetched from a more advanced replica (part.metadata_version >
# table.metadata_version). The part contains column h but the local table schema
# (at the lower metadata_version) has already dropped h.  A mutation unrelated to h
# must still complete without throwing LOGICAL_ERROR.
#
# We reproduce the state deterministically:
#   1. Create table (a, b, h).
#   2. Insert data → compact part P (metadata_version = MV0, has columns b and h).
#   3. Detach table; set P.metadata_version to MV0+2.
#   4. Reattach.
#   5. STOP MERGES so the DROP COLUMN mutation cannot execute yet.
#   6. ALTER DROP COLUMN h (schema advances to MV0+1, mutation queued but paused).
#   7. KILL that mutation so it never rewrites the part.
#   8. START MERGES.
#   9. Run ALTER UPDATE b = 1 WHERE 1 (a mutation that does NOT mention h).
#      Without the fix: splitAndModifyMutationCommands throws LOGICAL_ERROR because
#        part.metadata_version (MV0+2) > table.metadata_version (MV0+1) and h is not
#        in the table schema.
#      With the fix: the mutation completes successfully.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS rmt_meta_version SYNC;

    -- Compact parts (large min_bytes_for_wide_part) exercise the first branch of
    -- splitAndModifyMutationCommands where the version check lives.
    CREATE TABLE rmt_meta_version (a UInt64, b UInt64 DEFAULT 0, h String DEFAULT 'x')
    ENGINE = ReplicatedMergeTree('/clickhouse/{database}/rmt_meta_version', 'r1')
    ORDER BY a
    SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
"

# Insert data. The part is created at the current table metadata_version (MV0)
# and contains column h.
$CLICKHOUSE_CLIENT -q "INSERT INTO rmt_meta_version (a) SELECT number FROM numbers(100)"

# Obtain the part path while it still has column h.
PART_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'rmt_meta_version' AND active
    ORDER BY modification_time DESC
    LIMIT 1
")

# Safety: ensure path is absolute.
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    substring('$PART_PATH', 1, 1) != '/',
    'Expected absolute path, got: $PART_PATH')"

# Read the part's current metadata_version (MV0) from disk.
PART_MV=$(cat "${PART_PATH}metadata_version.txt")

# -----------------------------------------------------------------------
# Simulate fetching this part from a more advanced replica.
# After DROP COLUMN h the table will be at MV0+1.  We set the part's
# metadata_version to MV0+2 so it is strictly greater than the table's.
# -----------------------------------------------------------------------
ADVANCED_MV=$((PART_MV + 2))

$CLICKHOUSE_CLIENT -q "DETACH TABLE rmt_meta_version"
echo "$ADVANCED_MV" > "${PART_PATH}metadata_version.txt"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE rmt_meta_version"

# Pause the background mutation executor so that the DROP COLUMN mutation
# that is about to be queued does not execute immediately.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES rmt_meta_version"

# Update the schema: table advances to MV0+1, column h is removed from the
# schema.  The corresponding MUTATE_PART task is queued but cannot run yet.
$CLICKHOUSE_CLIENT -q "ALTER TABLE rmt_meta_version DROP COLUMN h SETTINGS alter_sync=0"

# Wait until the schema change has been applied locally (h disappears from
# system.columns) while the mutation is still queued.
for _ in {1..60}; do
    H_COUNT=$($CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM system.columns
        WHERE database = currentDatabase()
          AND table    = 'rmt_meta_version'
          AND name     = 'h'
    ")
    [ "$H_COUNT" -eq 0 ] && break
    sleep 0.2
done

# Cancel the DROP COLUMN mutation.  The part is never rewritten, so it
# continues to contain column h with metadata_version = ADVANCED_MV.
$CLICKHOUSE_CLIENT -q "
    KILL MUTATION WHERE
        database = currentDatabase()
        AND table = 'rmt_meta_version'
        AND command LIKE '%DROP COLUMN h%'
    SYNC
" 2>/dev/null || true

# Allow background processing to resume.
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES rmt_meta_version"

# -----------------------------------------------------------------------
# Trigger the bug: run a mutation that does NOT mention column h.
# State at this point:
#   • part.metadata_version  = ADVANCED_MV  (= MV0+2)
#   • table.metadata_version = MV0+1
#   • part contains column h; table schema does not
#
# Without the fix  → LOGICAL_ERROR in splitAndModifyMutationCommands
# With the fix     → mutation completes successfully
# -----------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE rmt_meta_version UPDATE b = 1 WHERE 1
    SETTINGS mutations_sync=0
"

# Poll for the UPDATE mutation to finish (up to ~30 s).
for _ in {1..150}; do
    DONE=$($CLICKHOUSE_CLIENT -q "
        SELECT min(is_done)
        FROM system.mutations
        WHERE database = currentDatabase()
          AND table = 'rmt_meta_version'
          AND command LIKE '%UPDATE b = 1 WHERE 1%'
    ")
    [ "$DONE" = "1" ] && break
    sleep 0.2
done

# Report outcome.  Expected (with fix): is_done=1, had_logical_error=0
$CLICKHOUSE_CLIENT -m -q "
    SELECT
        is_done,
        (latest_fail_reason LIKE '%LOGICAL_ERROR%') AS had_logical_error
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table    = 'rmt_meta_version'
      AND command  LIKE '%UPDATE b = 1 WHERE 1%'
    ORDER BY create_time
    LIMIT 1;

    -- Data must be intact.
    SELECT count() FROM rmt_meta_version;

    DROP TABLE rmt_meta_version SYNC;
"
