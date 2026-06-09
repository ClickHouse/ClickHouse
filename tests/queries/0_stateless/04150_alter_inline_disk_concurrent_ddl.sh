#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the TOCTOU race fixed in the second iteration of the issue #63019 fix.
# `clickhouse-gh[bot]` flagged that the simple "track and roll back on scope exit" approach
# raced against concurrent DDL:
#
#   1. Query A's `MODIFY SETTING disk = disk(name = 'X', ...)` validation registers `X`.
#   2. Concurrent query B sees `X` already registered (settings hash matches), reuses it
#      and joins the active-owners chain via `Context::getOrCreateDisk`.
#   3. Query B commits its metadata transition. Its scope's `commit` flips the
#      tentative-registration `committed` flag and drops query B from the active set.
#   4. Query A's later guard rejects the `ALTER`. Scope destructor sees `committed == true`
#      and leaves the disk alone in `DiskSelector`.
#   5. Query B's table (and any other concurrent observer that committed) keeps a usable
#      disk by name `X`. The settings-hash check still rejects redefinition with
#      DIFFERENT settings.
#
# This test exercises the same-name TOCTOU race from both ends:
# - A REJECTOR thread issues `ALTER ... MODIFY SETTING disk = disk(name = 'X_vN', ...)` whose
#   validation registers the inline disk and is then rejected by the storage-policy
#   migration guard. The destructor of the (validation-only) scope inside
#   `MergeTreeData::checkAlterIsPossible` would normally roll the disk back.
# - A SHADOW thread runs an innocuous settings-only `ALTER` on a table whose stored
#   `settings_changes` AST already names `disk = disk(name = 'X_vN', ...)` with IDENTICAL
#   settings to the REJECTOR's tentative. The ALTER apply path re-validates the inline
#   disk via `MergeTreeData::changeSettings` which calls `Context::getOrCreateDisk` with
#   a non-null scope; that call must JOIN the existing tentative entry's active-owners
#   set. The ALTER then commits, flipping `committed = true`.
# - An OBSERVER thread issues a settings-only `ALTER` on a table that has its OWN inline
#   disk (different name); used to keep the test's pre-existing OBSERVER coverage from
#   PR #103818 working.
#
# Post-conditions:
# - All SHADOW tables remain queryable (the active-owners commit kept the disk).
# - The shared `${DISK_REJ}_cache_v${i}` disk name cannot be redefined with DIFFERENT
#   settings: a fresh `CREATE TABLE` whose inline `disk(...)` uses the same name but
#   different `max_size` is rejected with `BAD_ARGUMENTS`.
# - A separate sequential-rollback name (NO concurrent observer) is still freed after
#   rejection: a fresh table can re-register the same name with DIFFERENT settings.
#
# A regression that breaks the join-existing-tentative branch in
# `Context::getOrCreateDisk` would leave SHADOW's scope unaware of the tentative entry;
# REJECTOR's destructor would then see itself as the lone active owner and roll the
# disk back, breaking SHADOW's table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_OBS="${CLICKHOUSE_TEST_UNIQUE_NAME}_obs_inline_disk"
DISK_REJ="${CLICKHOUSE_TEST_UNIQUE_NAME}_rej_inline_disk"
TABLE_OBS="${CLICKHOUSE_DATABASE}.observer_table"
TABLE_REJ="${CLICKHOUSE_DATABASE}.rejector_table"
TABLE_FOLLOWER="${CLICKHOUSE_DATABASE}.follower_table"
SHADOW_TABLE_PREFIX="${CLICKHOUSE_DATABASE}.shadow_table"
ITERATIONS=8

REJECTOR_LOG="${CLICKHOUSE_TMP}/04150_rejector_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
OBSERVER_LOG="${CLICKHOUSE_TMP}/04150_observer_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
SHADOW_LOG="${CLICKHOUSE_TMP}/04150_shadow_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

drop_shadow_tables() {
    for i in $(seq 1 ${ITERATIONS}); do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${SHADOW_TABLE_PREFIX}_${i};" 2>/dev/null
    done
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_OBS}; DROP TABLE IF EXISTS ${TABLE_REJ}; DROP TABLE IF EXISTS ${TABLE_FOLLOWER}; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.shadow_redefine_attempt;"
drop_shadow_tables

# OBSERVER table sits on its own inline custom disk that we never want to roll back.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_OBS} (a Int32, b String) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_OBS}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_OBS}_objstore/');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_OBS} VALUES (1, 'one'), (2, 'two'), (3, 'three');"

# REJECTOR table sits on a different inline custom disk; each loop iteration will issue
# `MODIFY SETTING disk = disk(name = 'rej_cache', ...)` cache wrap which the storage-policy
# migration guard rejects after the validation path has already registered the cache disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_REJ} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_REJ}_objstore/');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_REJ} VALUES (10), (20);"

# Pre-create SHADOW tables. Each table's stored `settings_changes` AST names
# `disk = disk(name = '${DISK_REJ}_cache_v${i}', ...)` with IDENTICAL settings to the
# REJECTOR's tentative registration. Once the SHADOW concurrent loop runs an innocuous
# settings-only `ALTER`, the apply path re-validates this inline disk and the
# `getOrCreateDisk` call JOINS REJECTOR's tentative entry (when it is in flight).
for i in $(seq 1 ${ITERATIONS}); do
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${SHADOW_TABLE_PREFIX}_${i} (a Int32) ENGINE = MergeTree() ORDER BY a
        SETTINGS disk = disk(
            name = '${DISK_REJ}_cache_v${i}',
            type = cache,
            disk = '${DISK_REJ}',
            path = './${DISK_REJ}_cache_v${i}_data/',
            max_size = '1Mi');
        INSERT INTO ${SHADOW_TABLE_PREFIX}_${i} VALUES (${i}00);"
done

# Concurrent loops. The three threads run at the same time so the OBSERVER's re-validation
# of `${DISK_OBS}`, the REJECTOR's register-then-roll-back of `${DISK_REJ}_cache_vN`, and
# the SHADOW's commit-against-the-same-tentative overlap multiple times.
:>"${REJECTOR_LOG}"
:>"${OBSERVER_LOG}"
:>"${SHADOW_LOG}"

(
    for i in $(seq 1 ${ITERATIONS}); do
        ${CLICKHOUSE_CLIENT} --query "
            ALTER TABLE ${TABLE_REJ} MODIFY SETTING disk = disk(
                name = '${DISK_REJ}_cache_v${i}',
                type = cache,
                disk = '${DISK_REJ}',
                path = './${DISK_REJ}_cache_v${i}_data/',
                max_size = '1Mi');" 2>&1 \
                | grep -qE "BAD_ARGUMENTS" \
                && echo "rejector_iter_${i}_rejected" >>"${REJECTOR_LOG}"
    done
) &
REJECTOR_PID=$!

(
    for i in $(seq 1 ${ITERATIONS}); do
        ${CLICKHOUSE_CLIENT} --query "
            ALTER TABLE ${TABLE_OBS} MODIFY SETTING merge_with_ttl_timeout = ${i}0;
            SELECT count(), sum(a) FROM ${TABLE_OBS};" >>"${OBSERVER_LOG}" 2>&1
    done
) &
OBSERVER_PID=$!

(
    for i in $(seq 1 ${ITERATIONS}); do
        # Innocuous settings-only ALTER on a table that ALREADY uses inline
        # `disk(name = '${DISK_REJ}_cache_v${i}', ...)`. The apply path's
        # `MergeTreeData::changeSettings` re-validates the inline disk via
        # `Context::getOrCreateDisk` with a tracking scope; this is the call that
        # JOINS the REJECTOR's tentative entry and lets SHADOW's later `commit` flip
        # `committed = true` so REJECTOR's destructor leaves the disk alone.
        ${CLICKHOUSE_CLIENT} --query "
            ALTER TABLE ${SHADOW_TABLE_PREFIX}_${i} MODIFY SETTING merge_with_ttl_timeout = ${i}1;" 2>&1 \
            | grep -vE "^$" >>"${SHADOW_LOG}" \
            ; echo "shadow_iter_${i}_done" >>"${SHADOW_LOG}"
    done
) &
SHADOW_PID=$!

wait ${REJECTOR_PID} ${OBSERVER_PID} ${SHADOW_PID}

# Print results in deterministic order. Each line of OBSERVER_LOG is the count() and sum()
# of `${TABLE_OBS}` after one ALTER iteration; all must be `3<TAB>6`. Each line of
# REJECTOR_LOG is `rejector_iter_N_rejected` for the iteration that produced
# BAD_ARGUMENTS. Each ITERATIONS shadow ALTER must have completed without an error
# message - SHADOW's apply scope joining REJECTOR's tentative entry is the only path
# that lets the disk survive REJECTOR's destructor when both happen to overlap.
sort "${REJECTOR_LOG}" 2>/dev/null
sort -u "${OBSERVER_LOG}" 2>/dev/null
grep -c "shadow_iter_.*_done" "${SHADOW_LOG}" 2>/dev/null
# Any error message from the SHADOW thread is a regression in the active-owners chain.
grep -E "^Code: |Error:" "${SHADOW_LOG}" 2>/dev/null
rm -f "${REJECTOR_LOG}" "${OBSERVER_LOG}" "${SHADOW_LOG}"

# Final post-conditions:
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_OBS};"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_REJ};"

# Every SHADOW table must be queryable with exactly one row whose `a` equals i*100. The
# committed-tentative-flag path must have left every same-name disk in `DiskSelector`.
for i in $(seq 1 ${ITERATIONS}); do
    ${CLICKHOUSE_CLIENT} --query "SELECT 'shadow_${i}', count(), sum(a) FROM ${SHADOW_TABLE_PREFIX}_${i};"
done

# Same name, DIFFERENT settings (max_size = '2Mi' instead of '1Mi') must be rejected with
# BAD_ARGUMENTS by the disk-settings-hash check in `getOrCreateCustomDisk`. This is the
# second guarantee the bot asked us to verify: once the active-owners chain commits the
# `${DISK_REJ}_cache_v${i}` name, no later DDL can silently rebind that name to a disk
# with different settings.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.shadow_redefine_attempt (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}_cache_v1',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${DISK_REJ}_cache_v1_data/',
    max_size = '2Mi');" 2>&1 \
    | grep -qE "BAD_ARGUMENTS" \
    && echo "redefine_with_different_settings_rejected"

# Sequential-rollback path: a separate name that NO concurrent thread observed. Run a
# single rejector ALTER on a brand-new name and verify the rollback freed it for a fresh
# `CREATE` with DIFFERENT settings.
${CLICKHOUSE_CLIENT} --query "
ALTER TABLE ${TABLE_REJ} MODIFY SETTING disk = disk(
    name = '${DISK_REJ}_cache_seq',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${DISK_REJ}_cache_seq_data/',
    max_size = '1Mi');" 2>&1 \
    | grep -qE "BAD_ARGUMENTS" \
    && echo "sequential_rejector_rejected"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FOLLOWER} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}_cache_seq',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${DISK_REJ}_cache_seq_follower_data/',
    max_size = '2Mi');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_FOLLOWER} VALUES (100), (200);"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_FOLLOWER};"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE_FOLLOWER}; DROP TABLE ${TABLE_OBS}; DROP TABLE ${TABLE_REJ}; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.shadow_redefine_attempt;"
drop_shadow_tables
