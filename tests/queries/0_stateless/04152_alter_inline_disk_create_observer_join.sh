#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Deterministic regression test for the same-name TOCTOU race that
# `clickhouse-gh[bot]` flagged on PR #103818 (issue #63019, commit 328ce5dc77e0):
# the unscoped CREATE / ATTACH path of `disk = disk(name = 'X', ...)` could
# observe a tentative `X` registered by an in-flight ALTER's validation scope and
# silently commit metadata against it; if the ALTER then rolled back, CREATE was
# left referencing a removed disk.
#
# This test uses the `disk_from_ast_pause_after_tentative_registration` failpoint
# to drive the race deterministically:
#
#   1. SHADOW thread: enable the failpoint and run an `ALTER ... MODIFY SETTING
#      disk = disk(name = 'cache', ...)` on a table whose storage policy
#      cannot accept the new disk. The validation scope inside
#      `MergeTreeData::checkAlterIsPossible` reaches `getOrCreateCustomDisk` and
#      pauses with the tentative `${DISK_REJ}_cache` registered.
#
#   2. Main thread: wait for the SHADOW pause, then run a `CREATE TABLE` whose
#      inline `disk(name = '${DISK_REJ}_cache', ...)` observes the tentative
#      entry. The unscoped CREATE-path observer in `Context::getOrCreateDisk`
#      eagerly marks the tentative entry committed.
#
#   3. Main thread: notify the failpoint to resume the SHADOW thread. Its
#      validation scope's destructor sees `committed = true` and leaves the disk
#      alone in `DiskSelector` / `FileCacheFactory`.
#
#   4. The SHADOW table must be queryable. Without the unscoped-observer fix the
#      validation scope's destructor would roll back the disk and the SELECT
#      would fail with `UNKNOWN_DISK` or similar.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_REJ="${CLICKHOUSE_TEST_UNIQUE_NAME}_rej"
TABLE_REJ="${CLICKHOUSE_DATABASE}.rejector_table"
SHADOW_TABLE="${CLICKHOUSE_DATABASE}.shadow_table"
ALTER_LOG="${CLICKHOUSE_TMP}/04152_alter_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REJ}; DROP TABLE IF EXISTS ${SHADOW_TABLE};"

# REJECTOR table has its own object-storage disk; the storage-policy migration
# guard rejects switching its `disk` setting to a `cache` wrap, but the rejection
# fires AFTER the inline cache disk has been validated and tentatively
# registered, which is exactly the window the failpoint pauses.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_REJ} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_REJ}_objstore/');"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;"

# REJECTOR ALTER runs in the background and pauses inside `getOrCreateCustomDisk`
# AFTER the tentative registration is recorded.
(
    ${CLICKHOUSE_CLIENT} --query "
        ALTER TABLE ${TABLE_REJ} MODIFY SETTING disk = disk(
            name = '${DISK_REJ}_cache',
            type = cache,
            disk = '${DISK_REJ}',
            path = './${DISK_REJ}_cache_data/',
            max_size = '1Mi');" >"${ALTER_LOG}" 2>&1
) &
ALTER_PID=$!

# Wait for the REJECTOR to pause at the failpoint.
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_pause_after_tentative_registration PAUSE;"

# At this point the tentative `${DISK_REJ}_cache` registration is in flight. A
# concurrent unscoped CREATE that observes it must eagerly commit so the
# REJECTOR's destructor cannot roll the disk back.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${SHADOW_TABLE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}_cache',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${DISK_REJ}_cache_data/',
    max_size = '1Mi');
INSERT INTO ${SHADOW_TABLE} VALUES (42);"

# Resume the REJECTOR; its validation scope's destructor runs.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration;"

wait ${ALTER_PID}

# Verify outcomes.
echo -n "alter_rejected: "
grep -qE "BAD_ARGUMENTS" "${ALTER_LOG}" && echo yes || echo no
echo -n "shadow_queryable: "
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${SHADOW_TABLE} FORMAT TabSeparated" 2>&1 \
    | tr -d '\n'
echo

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${SHADOW_TABLE}; DROP TABLE ${TABLE_REJ};"
rm -f "${ALTER_LOG}"
