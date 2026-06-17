#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the co-owner interference that clickhouse-gh[bot]
# flagged on PR #103818 (commit a7980f0c78de, 2026-06-09T14:41Z).
#
# Ownership model. A normal `CREATE TABLE ... SETTINGS disk = disk(...)` runs under the
# ambient `CustomDiskRegistrationScope` installed by `InterpreterCreateQuery`, so each
# observing CREATE joins the disk's `active_owners` as an INDEPENDENT scoped owner (its own
# distinct scope pointer), like the ALTER apply scope. The disk is rolled back only when
# the LAST active owner leaves AND none has committed. This test proves co-owners are
# independent: one owner failing and rolling its own slot back must not tear out another
# co-owner's slot.
#
# The interference the bot described (against the earlier per-entry scheme on this branch):
#   1. Scoped ALTER A registers X (active_owners = [A]).
#   2. CREATE B (different settings) and CREATE C (matching settings) both observe X.
#   3. B fails the settings-hash check and releases; a shared-value release could remove
#      BOTH B's and C's slots at once, so C's slot vanished.
#   4. A then rejects and, seeing active_owners empty, rolled X back from DiskSelector while
#      C was still validating / about to commit metadata. C then committed against a disk
#      name already removed from the selector (its getDisk(X) re-resolution fails with
#      UNKNOWN_DISK).
#
# The fix gives each owner an independent slot in `active_owners` keyed by its own scope
# pointer (`removePendingCustomDiskIfOwned` erases only that pointer), so B's release can
# no longer drop C's slot. The rollback waits until `active_owners` is empty.
#
# Driven deterministically with two PAUSEABLE_ONCE failpoints:
#   1. disk_from_ast_pause_after_tentative_registration: pauses the FIRST caller
#      (scoped ALTER A) after the tentative registration, before validation.
#   2. disk_from_ast_unscoped_observer_pause_after_sentinel: pauses the FIRST
#      observing CREATE (CREATE C, launched before B) after it joins the active
#      owners, before validation. Gated on the explicit `scope` parameter (null
#      for CREATE / ATTACH), so it never pauses the ALTER apply path.
#
# Sequence:
#   1. ALTER A pauses at failpoint #1. active_owners = [A].
#   2. CREATE C (matching settings S1) observes X under ScopeC: active_owners = [A, C]. C
#      reaches failpoint #1 (consumed by A, no pause), then pauses at #2.
#   3. CREATE B (different settings S2) observes X under ScopeB: active_owners = [A, C, B].
#      B reaches #1 and #2 (both consumed, no pause), runs validation, fails the
#      settings-hash check, and ScopeB's destructor removes only B: active_owners = [A, C].
#   4. Notify A. Validation succeeds; the storage-policy migration guard rejects the ALTER;
#      ~CustomDiskRegistrationScope removes A: active_owners = [C]. With the fix the rollback
#      is DEFERRED (C is still in flight). With the bug C's slot was already gone, so the
#      rollback removed X here.
#   5. Notify C. Validation passes (settings match); ScopeC commits. C re-resolves
#      getDisk(X) and creates its table. With the fix X is still registered, so C succeeds.
#      With the bug X was removed in step 4, so C fails with UNKNOWN_DISK and the table is
#      never created.
#
# This exercises co-owner independence in the scoped/ambient `active_owners` path. It does
# NOT cover the no-ambient-scope fallback (`unscoped_observers` +
# `releaseUnscopedDiskObservation` / `commitUnscopedDiskObservation`), which is now reached
# only when an inline `disk(...)` is converted with no ambient scope installed - i.e.
# background metadata / lazy loading, not interactive DDL.
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

# Defense-in-depth cleanup. This test enables two server-wide pauseable failpoints and
# pauses two background clients. If anything before the success tail fails (e.g. a
# `SYSTEM WAIT FAILPOINT ... PAUSE` timeout, a foreground query, or a `wait`), a failpoint
# could stay enabled or a background client could stay paused, disrupting later tests on
# the same server. On exit, notify+disable both failpoints (so any paused client resumes
# and exits), kill any background PIDs, and drop the tables. `NOTIFY`/`DISABLE` on an
# inactive failpoint and DROP IF EXISTS are no-ops, so this is safe alongside the normal
# cleanup tail.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_pause_after_tentative_registration" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel" 2>/dev/null || true
    for p in "${ALTER_PID:-}" "${MATCH_PID:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_MATCH}; DROP TABLE IF EXISTS ${TABLE_MISMATCH}; DROP TABLE IF EXISTS ${TABLE_ALTER};" 2>/dev/null || true
    rm -f "${ALTER_LOG}" "${MATCH_LOG}" "${MISMATCH_LOG}"
' EXIT

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
# and pauses at failpoint #1. ScopeA is alive on the validation thread's stack;
# active_owners = [A].
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

# CREATE C observes X with MATCHING settings S1 (max_size='1Mi') under its own ambient
# ScopeC, joining active_owners = [A, C], and pauses at failpoint #2.
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

# CREATE B observes X with DIFFERENT settings S2 (max_size='2Mi') under its own ambient
# ScopeB, joining active_owners = [A, C, B]. Both failpoints are already consumed, so B
# does not pause: it runs straight to the settings-hash check, fails with BAD_ARGUMENTS,
# and ScopeB's destructor removes only B (active_owners = [A, C]).
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_MISMATCH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');" >"${MISMATCH_LOG}" 2>&1

# Resume ALTER A. Validation succeeds, the storage-policy migration guard rejects
# the ALTER, and ~CustomDiskRegistrationScope removes A from active_owners (now [C]). With
# the fix C's still-present slot defers the rollback; with the bug C's slot was already
# erased by B, so the rollback removes X here while C is paused.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration;"
wait ${ALTER_PID}

# Resume CREATE C. With the fix X is still registered: validation passes, ScopeC commits,
# getDisk(X) re-resolves, and the table is created. With the bug X was removed in the
# previous step, so C fails with UNKNOWN_DISK.
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
