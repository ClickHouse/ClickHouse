#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Deterministic regression test for the reverse-order leak that
# `clickhouse-gh[bot]` flagged on PR #103818 (commit `f7e54c384529`,
# 2026-06-09T09:28Z).
#
# Ownership model. A normal `CREATE TABLE ... SETTINGS disk = disk(...)` runs under the
# ambient `CustomDiskRegistrationScope` that `InterpreterCreateQuery::doCreateTable`
# installs, so `getOrCreateCustomDisk` sees a non-null effective scope and the CREATE
# joins the disk's `active_owners` as an independent scoped owner (its own scope pointer),
# exactly like the ALTER apply scope. The disk is rolled back only when the LAST active
# owner leaves AND none has committed. This test drives the case where the observing
# CREATE is the last owner to leave.
#
# In the forward order (covered by 04153) the observing CREATE's scope destructor runs
# BEFORE the ALTER's: it removes its own slot, the ALTER's slot is still present, so the
# rollback is deferred to the ALTER.
#
# In the reverse order (this test) the scoped ALTER's destructor runs FIRST. It removes
# itself from `active_owners`, finds the CREATE's slot still present, and defers the
# rollback ("not the last owner"). The observing CREATE then fails validation and its
# scope destructor runs as the last owner with an uncommitted entry. Without the fix that
# final last-owner-uncommitted release only erased the bookkeeping entry; the disk and any
# owned cache alias stayed in `DiskSelector` / `FileCacheFactory` until restart, and any
# later DDL with the same name and different settings was rejected with `BAD_ARGUMENTS`
# (settings-hash mismatch with the leaked entry).
#
# The race is driven deterministically with two `PAUSEABLE_ONCE` failpoints:
#   1. `disk_from_ast_pause_after_tentative_registration` (existing): pauses
#      the FIRST caller (the scoped ALTER) inside `getOrCreateCustomDisk`,
#      AFTER the tentative registration but BEFORE validation.
#   2. `disk_from_ast_unscoped_observer_pause_after_sentinel`: pauses the
#      observing CREATE inside `getOrCreateCustomDisk`, AFTER it has joined
#      the active owners but BEFORE validation. Gated on the explicit `scope`
#      parameter (null for CREATE / ATTACH), so it pauses a CREATE / ATTACH
#      observer but never the ALTER apply path (see DiskFromAST.cpp).
#
# Sequence of events:
#   1. ALTER A pauses at failpoint #1 with `active_owners = [ScopeA]`.
#   2. CREATE B observes the existing disk under its own ambient scope ScopeB:
#      `Context::getOrCreateDisk` joins it to `active_owners = [ScopeA, ScopeB]`.
#      CREATE B then reaches failpoint #1 (already consumed by A, no pause)
#      and pauses at failpoint #2 BEFORE validation.
#   3. Notify failpoint #1: ALTER A resumes. Its validation succeeds (own
#      registration, settings match). `getOrCreateCustomDisk` returns to
#      `MergeTreeData::changeSettings`. The storage-policy migration guard
#      rejects swapping a non-config disk for the existing object-storage
#      one, throwing `BAD_ARGUMENTS`. `~CustomDiskRegistrationScope` calls
#      `removePendingCustomDiskIfOwned`; owners go from
#      `[ScopeA, ScopeB]` to `[ScopeB]`, non-empty, rollback deferred.
#   4. Notify failpoint #2: CREATE B resumes. Its settings-hash check rejects
#      it with `BAD_ARGUMENTS` (S2 != S1). The exception unwinds ScopeB, whose
#      destructor calls `removePendingCustomDiskIfOwned`; owners go from
#      `[ScopeB]` to `[]`, uncommitted, last owner left.
#   5. With the fix: the last-owner-uncommitted branch performs the rollback
#      (erase entry, drop storage policy, remove cache alias if owned, shut
#      down the disk). The disk name is free.
#
# This exercises the scoped/ambient last-owner-leaves rollback. It does NOT cover the
# no-ambient-scope fallback (`unscoped_observers` + `releaseUnscopedDiskObservation` /
# `commitUnscopedDiskObservation`), which is now reached only when an inline `disk(...)`
# is converted with no ambient scope installed - i.e. background metadata / lazy loading
# (`MergeTreeSettings`/`DatabaseMetadataDiskSettings::loadFromQuery`), not interactive DDL.
#
# Verifies the fix by attempting a fresh CREATE with name X and a third set
# of settings S3 (different from both S1 and S2). With the fix it succeeds
# (rollback ran). Without the fix it fails with `BAD_ARGUMENTS` because
# the leaked entry's settings-hash blocks redefinition until restart.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_REJ="${CLICKHOUSE_TEST_UNIQUE_NAME}_rej"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_REJ="${CLICKHOUSE_DATABASE}.rejector_table"
TABLE_FRESH="${CLICKHOUSE_DATABASE}.fresh_table"
ALTER_LOG="${CLICKHOUSE_TMP}/04154_alter_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
CREATE_MISMATCH_LOG="${CLICKHOUSE_TMP}/04154_create_mismatch_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

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
    for p in "${ALTER_PID:-}" "${CREATE_PID:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mismatch_attempt; DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${TABLE_REJ};" 2>/dev/null || true
    rm -f "${ALTER_LOG}" "${CREATE_MISMATCH_LOG}"
' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REJ}; DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mismatch_attempt;"

# REJECTOR table on its own object-storage disk; the storage-policy
# migration guard rejects swapping it for a cache wrap, but only AFTER
# the validation path has tentatively registered the cache disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_REJ} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_REJ}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_REJ}_objstore/');"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_pause_after_tentative_registration;"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;"

# ALTER A pauses at failpoint #1 (PAUSEABLE_ONCE) with `${SHARED_NAME}` tentatively
# registered with settings S1 (`max_size = '1Mi'`). Scope A is alive on the validation
# thread's stack.
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

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_pause_after_tentative_registration PAUSE;"

# CREATE B observes the existing disk under its own ambient scope ScopeB:
# Context::getOrCreateDisk joins ScopeB to active_owners. CREATE B reaches failpoint #1
# (already consumed by A, no pause) and pauses at failpoint #2.
(
    ${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.mismatch_attempt (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_REJ}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');" >"${CREATE_MISMATCH_LOG}" 2>&1
) &
CREATE_PID=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel PAUSE;"

# Resume ALTER A first. Its validation succeeds, returns to `MergeTreeData::changeSettings`,
# storage-policy migration guard rejects, scope destructor runs, owners go from
# [ScopeA, ScopeB] to [ScopeB] - rollback deferred because ScopeB is still present.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_pause_after_tentative_registration;"
wait ${ALTER_PID}

# Now the entry is in the bug state: the ALTER owner is gone, CREATE B's ScopeB still
# pinned, uncommitted. Resume CREATE B: its settings-hash check rejects the CREATE, and
# ScopeB's destructor runs `removePendingCustomDiskIfOwned` as the last owner with empty
# active_owners and uncommitted. With the fix, it performs the rollback. Without the fix,
# the disk and any owned cache alias stay in the global selectors.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;"
wait ${CREATE_PID}

# Verify outcomes. Accept CANNOT_OPEN_FILE alongside BAD_ARGUMENTS: the rejector ALTER's
# inline cache-wrap disk is built during validation, rolled back, then rebuilt on the apply
# path; if the validation build's FileCache StatusFile flock is not yet released by its async
# destructor, the rebuild is rejected with CANNOT_OPEN_FILE instead of the storage-policy
# guard's BAD_ARGUMENTS. Both reject the ALTER and leave the table unchanged; accept either
# so the marker does not depend on cache-teardown timing (cf. 04150).
echo -n "alter_rejected: "
grep -qE "BAD_ARGUMENTS|CANNOT_OPEN_FILE" "${ALTER_LOG}" && echo yes || echo no
echo -n "mismatch_create_rejected: "
grep -qE "BAD_ARGUMENTS" "${CREATE_MISMATCH_LOG}" && echo yes || echo no

# The disk name must be free. A fresh CREATE with a THIRD set of settings S3
# (`max_size = '4Mi'`, different from both S1's `'1Mi'` and S2's `'2Mi'`) must
# succeed; if the bug had left the entry leaked with S1, the settings-hash check
# would reject this with BAD_ARGUMENTS until restart.
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
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT disk_from_ast_unscoped_observer_pause_after_sentinel;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mismatch_attempt; DROP TABLE ${TABLE_FRESH}; DROP TABLE ${TABLE_REJ};"
rm -f "${ALTER_LOG}" "${CREATE_MISMATCH_LOG}"
