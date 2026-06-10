#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the co-observer interference that clickhouse-gh[bot]
# flagged on PR #103818 (commit a7980f0c78de, 2026-06-09T14:41Z).
#
# The earlier reverse-order fix (04154) pinned an unscoped CREATE / ATTACH
# observer of a tentative custom-disk registration by pushing a single shared
# value (the entry address) into the tentative entry's active-owners list. The
# bug: EVERY unscoped observer of the same disk name pushed the SAME value, so
# when one observer dropped its pin via erase(remove(..., &entry)) it dropped
# EVERY co-observer's pin at once.
#
# Concrete interleaving the bot described:
#   1. Scoped ALTER A registers X (active_owners = [A]).
#   2. Unscoped CREATE B (different settings) and unscoped CREATE C (matching
#      settings) both observe X. With the shared-value scheme that made
#      active_owners = [A, sentinel, sentinel].
#   3. B fails the settings-hash check and releases; the shared-value erase
#      removed BOTH sentinels, so C's pin vanished.
#   4. A then rejects and, seeing active_owners empty, rolled X back from
#      DiskSelector while C was still validating / about to commit metadata.
#      C then committed against a disk name already removed from the selector
#      (its getDisk(X) re-resolution fails with UNKNOWN_DISK).
#
# The fix replaces the shared sentinel with a per-entry unscoped_observers
# refcount: each observer holds exactly one independent reference, so B's
# release can no longer drop C's. The rollback now waits until active_owners is
# empty AND unscoped_observers == 0.
#
# Driven deterministically with two PAUSEABLE_ONCE failpoints:
#   1. disk_from_ast_pause_after_tentative_registration: pauses the FIRST caller
#      (scoped ALTER A) after the tentative registration, before validation.
#   2. disk_from_ast_unscoped_observer_pause_after_sentinel: pauses the FIRST
#      unscoped observer (CREATE C, launched before B) after it takes its
#      reference, before validation. Fires only for unscoped callers.
#
# Sequence:
#   1. ALTER A pauses at failpoint #1. active_owners = [A], unscoped_observers = 0.
#   2. CREATE C (matching settings S1) observes X: unscoped_observers = 1. C
#      reaches failpoint #1 (consumed by A, no pause), then pauses at #2.
#   3. CREATE B (different settings S2) observes X: unscoped_observers = 2. B
#      reaches #1 and #2 (both consumed, no pause), runs validation, fails the
#      settings-hash check, releases: unscoped_observers = 1. B is rejected.
#   4. Notify A. Validation succeeds; the storage-policy migration guard rejects
#      the ALTER; ~CustomDiskRegistrationScope removes A from active_owners.
#      Now active_owners = [], unscoped_observers = 1 (C). With the fix the
#      rollback is DEFERRED (an observer is still in flight). With the bug C's
#      pin was already gone, so the rollback removed X here.
#   5. Notify C. Validation passes (settings match), commitUnscopedDiskObservation
#      flips committed. C re-resolves getDisk(X) and creates its table. With the
#      fix X is still registered, so C succeeds. With the bug X was removed in
#      step 4, so C fails with UNKNOWN_DISK and the table is never created.
#
# Verifies the fix by asserting C's table is created and queryable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_BASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_base"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_ALTER="${CLICKHOUSE_DATABASE}.alter_table"
TABLE_MATCH="${CLICKHOUSE_DATABASE}.match_table"
TABLE_MISMATCH="${CLICKHOUSE_DATABASE}.mismatch_table"
ALTER_LOG="${CLICKHOUSE_TMP}/04155_alter_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
MATCH_LOG="${CLICKHOUSE_TMP}/04155_match_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
MISMATCH_LOG="${CLICKHOUSE_TMP}/04155_mismatch_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_ALTER}; DROP TABLE IF EXISTS ${TABLE_MATCH}; DROP TABLE IF EXISTS ${TABLE_MISMATCH};"

# ALTER target on its own object-storage disk; the storage-policy migration
# guard rejects swapping it for a cache wrap, but only AFTER the validation
# path has tentatively registered the cache disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_ALTER} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_BASE}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_BASE}_objstore/');"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;"

# ALTER A tentatively registers ${SHARED_NAME} with settings S1 (max_size='1Mi')
# and pauses at failpoint #1. Scope A is alive on the validation thread's stack.
(
    ${CLICKHOUSE_CLIENT} --query "
        ALTER TABLE ${TABLE_ALTER} MODIFY SETTING disk = disk(
            name = '${SHARED_NAME}',
            type = cache,
            disk = '${DISK_BASE}',
            path = './${SHARED_NAME}_data/',
            max_size = '1Mi');" >"${ALTER_LOG}" 2>&1
) &
ALTER_PID=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_pause_after_tentative_registration PAUSE;"

# CREATE C observes X with MATCHING settings S1 (max_size='1Mi'), takes the
# first unscoped reference (unscoped_observers = 1), and pauses at failpoint #2.
(
    ${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_MATCH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '1Mi');
INSERT INTO ${TABLE_MATCH} VALUES (42);" >"${MATCH_LOG}" 2>&1
) &
MATCH_PID=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel PAUSE;"

# CREATE B observes X with DIFFERENT settings S2 (max_size='2Mi'), taking the
# second unscoped reference (unscoped_observers = 2). Both failpoints are already
# consumed, so B does not pause: it runs straight to the settings-hash check,
# fails with BAD_ARGUMENTS, and releases its reference (unscoped_observers = 1).
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_MISMATCH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');" >"${MISMATCH_LOG}" 2>&1

# Resume ALTER A. Validation succeeds, the storage-policy migration guard rejects
# the ALTER, and ~CustomDiskRegistrationScope removes A from active_owners. With
# the fix unscoped_observers == 1 (C) defers the rollback; with the bug C's pin
# was already erased by B, so the rollback removes X here while C is paused.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration;"
wait ${ALTER_PID}

# Resume CREATE C. With the fix X is still registered: validation passes, the
# observation commits, getDisk(X) re-resolves, and the table is created. With the
# bug X was removed in the previous step, so C fails with UNKNOWN_DISK.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;"
wait ${MATCH_PID}

echo -n "alter_rejected: "
grep -qE "BAD_ARGUMENTS" "${ALTER_LOG}" && echo yes || echo no
echo -n "mismatch_create_rejected: "
grep -qE "BAD_ARGUMENTS" "${MISMATCH_LOG}" && echo yes || echo no
echo -n "matching_create_succeeded: "
grep -qE "Code:|Exception" "${MATCH_LOG}" && echo no || echo yes

# The matching CREATE's table must be fully usable: it committed against X, which
# the fix kept registered. Without the fix this query errors (table never created).
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a) FROM ${TABLE_MATCH};"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_MATCH}; DROP TABLE IF EXISTS ${TABLE_MISMATCH}; DROP TABLE IF EXISTS ${TABLE_ALTER};"
rm -f "${ALTER_LOG}" "${MATCH_LOG}" "${MISMATCH_LOG}"
