#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Deterministic regression test for the Concern 1 hole that `clickhouse-gh[bot]`
# flagged on PR #103818 (commit `e739f465`, 2026-06-09T04:02Z): the unscoped
# CREATE / ATTACH path's eager `committed = true` flip happened BEFORE the
# settings-hash check in `getOrCreateCustomDisk`. A concurrent CREATE with the
# SAME name but DIFFERENT settings could mark a still-in-flight tentative entry
# committed, then itself be rejected, leaving the original scoped owner unable
# to roll the disk back.
#
# Race driven deterministically via `disk_from_ast_pause_after_tentative_registration`:
#
#   1. SHADOW thread: `ALTER ... MODIFY SETTING disk = disk(name = 'X', S1)` on a
#      table whose storage policy cannot accept the new disk. The validation
#      scope tentatively registers `X` with settings `S1` and pauses at the
#      failpoint.
#
#   2. Main thread: wait for the SHADOW pause, then run a `CREATE TABLE` whose
#      inline `disk(name = 'X', S2)` uses the SAME name but DIFFERENT settings
#      (`max_size = '2Mi'` vs SHADOW's `'1Mi'`). The unscoped CREATE-path
#      observer joins the tentative entry with a sentinel slot, then runs the
#      settings-hash check which rejects the CREATE with `BAD_ARGUMENTS`. The
#      sentinel must be released and the tentative entry must NOT be marked
#      committed.
#
#   3. Main thread: notify the failpoint to resume SHADOW. The SHADOW ALTER
#      reaches its storage-policy guard, is rejected with `BAD_ARGUMENTS`, and
#      its validation scope's destructor sees `committed = false`, owners empty
#      (sentinel removed), and rolls the disk back globally.
#
#   4. After SHADOW finishes, the disk name `X` must be free. A fresh CREATE
#      with name `X` and a THIRD set of settings `S3` (different from both `S1`
#      and `S2`) must succeed; this proves the rollback ran fully and the entry
#      is gone, not merely retained with `S1`.
#
# Without the fix:
#   - The unscoped CREATE in step 2 would have eagerly flipped `committed = true`
#     in `getOrCreateDisk` BEFORE the settings-hash check rejected it.
#   - SHADOW's destructor in step 3 would see `committed = true` and leak the
#     `X` registration with `S1` in `DiskSelector` / `FileCacheFactory`.
#   - The fresh CREATE in step 4 with `S3 != S1` would FAIL with `BAD_ARGUMENTS`
#     (settings-hash mismatch with the leaked entry, even though no table
#     references it) until server restart.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_REJ="${CLICKHOUSE_TEST_UNIQUE_NAME}_rej"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_REJ="${CLICKHOUSE_DATABASE}.rejector_table"
TABLE_FRESH="${CLICKHOUSE_DATABASE}.fresh_table"
ALTER_LOG="${CLICKHOUSE_TMP}/04153_alter_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
CREATE_MISMATCH_LOG="${CLICKHOUSE_TMP}/04153_create_mismatch_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REJ}; DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mismatch_attempt;"

# Set up a REJECTOR table on its own object-storage disk; the storage-policy
# migration guard will reject swapping its `disk` setting to a cache wrap, but
# only AFTER the validation path has tentatively registered the cache disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_REJ} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_REJ}_objstore/');"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;"

# SHADOW ALTER pauses at the failpoint with `${SHARED_NAME}` tentatively registered
# with settings S1 (`max_size = '1Mi'`).
(
    ${CLICKHOUSE_CLIENT} --query "
        ALTER TABLE ${TABLE_REJ} MODIFY SETTING disk = disk(
            name = '${SHARED_NAME}',
            type = cache,
            disk = '${DISK_REJ}',
            path = './${SHARED_NAME}_data/',
            max_size = '1Mi');" >"${ALTER_LOG}" 2>&1
) &
ALTER_PID=$!

# Wait for the REJECTOR to pause inside `getOrCreateCustomDisk`.
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_pause_after_tentative_registration PAUSE;"

# Concurrent unscoped CREATE with the SAME name but DIFFERENT settings (S2:
# `max_size = '2Mi'`). It must be rejected by the settings-hash check WITHOUT
# leaving the tentative entry marked committed.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.mismatch_attempt (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');" >"${CREATE_MISMATCH_LOG}" 2>&1

# Resume the REJECTOR; its validation scope's destructor runs.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration;"

wait ${ALTER_PID}

# Verify outcomes.
# 1. The mismatched-settings CREATE was rejected with BAD_ARGUMENTS.
echo -n "mismatch_create_rejected: "
grep -qE "BAD_ARGUMENTS" "${CREATE_MISMATCH_LOG}" && echo yes || echo no
# 2. The SHADOW ALTER was also rejected.
echo -n "alter_rejected: "
grep -qE "BAD_ARGUMENTS" "${ALTER_LOG}" && echo yes || echo no

# 3. The disk name must be free now (no leaked tentative entry). A fresh CREATE
# with a THIRD set of settings S3 (`max_size = '4Mi'`, different from both S1's
# `'1Mi'` and S2's `'2Mi'`) must succeed; if the bug had left the entry committed
# with S1, the settings-hash check would reject this CREATE with BAD_ARGUMENTS.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FRESH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${SHARED_NAME}_data/',
    max_size = '4Mi');
INSERT INTO ${TABLE_FRESH} VALUES (42);
SELECT count(), sum(a) FROM ${TABLE_FRESH};"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mismatch_attempt; DROP TABLE ${TABLE_FRESH}; DROP TABLE ${TABLE_REJ};"
rm -f "${ALTER_LOG}" "${CREATE_MISMATCH_LOG}"
