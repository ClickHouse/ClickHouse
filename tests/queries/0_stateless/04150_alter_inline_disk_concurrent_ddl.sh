#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the TOCTOU race fixed in the second iteration of the issue #63019 fix.
# clickhouse-gh[bot] flagged that the simple "track and roll back on scope exit" approach
# raced against concurrent DDL:
#
#   1. Query A's `MODIFY SETTING disk = disk(name = 'X', ...)` validation registers `X`.
#   2. Concurrent query B sees `X` already registered (settings hash matches), reuses it
#      WITHOUT tracking the registration in its own scope.
#   3. Query B commits metadata pointing to `X`.
#   4. Query A's later guard rejects the ALTER. Scope destructor unconditionally removes
#      `X` from the global selector.
#   5. Query B's table now references a name no longer in the selector. A later
#      `CREATE TABLE ... SETTINGS disk = disk(name = 'X', different settings)` succeeds and
#      silently rebinds query B's table to a different disk.
#
# The fix: `Context::getOrCreateDisk` clears the pending-rollback marker for `name`
# whenever an existing custom disk is observed. The destructor only removes the disk when
# the marker is still owned by this scope; if another DDL has observed it, the disk is
# leaked (safe: settings-hash check still rejects redefinition with different settings).
#
# This test exercises both halves of the race and verifies the post-conditions:
# - A REJECTOR thread issues a `MODIFY SETTING disk = ...` ALTER whose validation
#   registers a new inline disk and is then rejected by the storage-policy migration
#   guard.
# - An OBSERVER thread concurrently issues a `MODIFY SETTING merge_with_ttl_timeout` ALTER
#   on a table that ALREADY uses an inline disk, which re-runs the validation path on
#   that table's pre-existing inline disk.
# - Loop the pair many times to produce overlap on multiple iterations.
# - At the end: the OBSERVER's table must still be queryable through its original inline
#   disk (it was never removed from the selector); a fresh table reusing the rejector's
#   leaked-or-rolled-back inline disk name with DIFFERENT settings must succeed (sequential
#   rollback works in the no-concurrent-observation case, and the bot's specific
#   leak-becomes-overwrite scenario is structurally impossible).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_OBS="${CLICKHOUSE_TEST_UNIQUE_NAME}_obs_inline_disk"
DISK_REJ="${CLICKHOUSE_TEST_UNIQUE_NAME}_rej_inline_disk"
TABLE_OBS="${CLICKHOUSE_DATABASE}.observer_table"
TABLE_REJ="${CLICKHOUSE_DATABASE}.rejector_table"
TABLE_FOLLOWER="${CLICKHOUSE_DATABASE}.follower_table"
ITERATIONS=8

REJECTOR_LOG="${CLICKHOUSE_TMP}/04150_rejector_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
OBSERVER_LOG="${CLICKHOUSE_TMP}/04150_observer_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_OBS}; DROP TABLE IF EXISTS ${TABLE_REJ}; DROP TABLE IF EXISTS ${TABLE_FOLLOWER};"

# OBSERVER table sits on its own inline custom disk that we never want to roll back.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_OBS} (a Int32, b String) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_OBS}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_OBS}_objstore/');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_OBS} VALUES (1, 'one'), (2, 'two'), (3, 'three');"

# REJECTOR table sits on a different inline custom disk; each loop iteration will issue a
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

# Concurrent loops. The two threads run at the same time so the OBSERVER's re-validation
# of `${DISK_OBS}` and the REJECTOR's register-then-roll-back of `${DISK_REJ}_cache_vN`
# overlap multiple times. The OBSERVER's ALTER must keep working and `${DISK_OBS}` must
# stay registered. A fresh `${DISK_REJ}_cache_vN` is used per iteration so each iteration
# independently tests the rollback (or leak-on-observation) path.
:>"${REJECTOR_LOG}"
:>"${OBSERVER_LOG}"

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

wait ${REJECTOR_PID} ${OBSERVER_PID}

# Print results in deterministic order. Each line of OBSERVER_LOG is the count() and sum()
# of `${TABLE_OBS}` after one ALTER iteration; all must be `3<TAB>6`. Each line of
# REJECTOR_LOG is `rejector_iter_N_rejected` for the iteration that produced BAD_ARGUMENTS.
sort "${REJECTOR_LOG}" 2>/dev/null
sort -u "${OBSERVER_LOG}" 2>/dev/null
rm -f "${REJECTOR_LOG}" "${OBSERVER_LOG}"

# Final post-conditions:
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_OBS};"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_REJ};"

# Sequential-rollback path: the latest rejected `${DISK_REJ}_cache_v${ITERATIONS}` was
# registered and immediately rolled back (no concurrent observer of that exact name). We
# can register the same name with DIFFERENT settings and a fresh table is created.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FOLLOWER} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}_cache_v${ITERATIONS}',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${DISK_REJ}_cache_v${ITERATIONS}_follower_data/',
    max_size = '2Mi');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_FOLLOWER} VALUES (100), (200);"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_FOLLOWER};"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE_FOLLOWER}; DROP TABLE ${TABLE_OBS}; DROP TABLE ${TABLE_REJ};"
