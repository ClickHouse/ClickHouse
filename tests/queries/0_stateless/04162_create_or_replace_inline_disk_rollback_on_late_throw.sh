#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the CREATE OR REPLACE leak that clickhouse-gh[bot] flagged on
# PR #103818 (src/Interpreters/InterpreterCreateQuery.cpp:2238, 2026-06-17T07:34Z).
#
# CREATE OR REPLACE TABLE runs through `doCreateOrReplaceTable`, which first creates a
# randomly-named temporary table via the inner `doCreateTable`, then runs
# `fillTableIfNeeded` and the final rename/exchange. The inner create makes the
# temporary table's metadata durable and used to commit the ambient
# `CustomDiskRegistrationScope` right there. But the user-visible DDL is durable only
# after the final rename/exchange. If the fill or rename failed, the catch in
# `doCreateOrReplaceTable` dropped the temporary table, yet the inline custom disk had
# already been committed globally - so the failed CREATE OR REPLACE left the disk name
# reserved in `DiskSelector` / `FileCacheFactory` with no table owning it. A later valid
# CREATE with the same disk name and different settings was then rejected by the
# settings-hash check until server restart.
#
# The fix makes the outer `doCreateOrReplaceTable` own the ambient scope across the
# temporary create + fill + rename/exchange and commit only after the rename succeeds.
# The inner `doCreateTable` is told the caller owns the scope, so it neither installs a
# nested scope nor commits the disk early. On the catch path the temporary table is
# dropped while the scope is still uncommitted, so its destructor rolls the disk back.
#
# This drives the late failure deterministically with a ONCE failpoint
# `create_or_replace_fail_after_inner_create`, which throws inside
# `doCreateOrReplaceTable` after the inner create succeeded and before the rename.
#
# Sequence:
#   1. Create a base object-storage disk via a committed table.
#   2. Enable the failpoint. CREATE OR REPLACE TABLE t with a fresh inline cache disk X
#      (name N, settings S1). The inner create registers X under the caller-owned scope,
#      then the failpoint throws before the rename. With the fix the scope rolls the
#      registration of N back from DiskSelector / FileCacheFactory; without the fix N
#      leaks committed with settings S1.
#   3. Disable the failpoint. CREATE TABLE t2 reusing the SAME disk name N but DIFFERENT
#      settings S2 (different max_size). With the fix N's registration was rolled back, so
#      this is a fresh registration and succeeds. Without the fix the leaked S1 entry's
#      settings-hash rejects S2 with BAD_ARGUMENTS until restart.
#
# Note on the cache disk path. The rolled-back CREATE OR REPLACE creates the temporary
# table fully (its metadata is durable) before the failpoint throws; the catch path drops
# that temporary table, but under an Atomic database the physical drop is delayed
# (`database_atomic_delay_before_drop_table_sec`), so the dropped table's FileCache keeps
# the on-disk cache `status` lock for that window. That is intrinsic Atomic delayed-drop
# behavior, independent of the registration leak under test. To exercise the registration
# rollback (the actual bug) without colliding on that physical lock, t2 reuses the disk
# NAME with a DIFFERENT cache path. The settings-hash check keys on the disk name, so name
# reuse with different settings is exactly what the leak would have blocked.
#
# Verifies the fix by asserting the CREATE OR REPLACE failed with the injected fault, and
# t2 (same disk name, different settings/path) was created and is queryable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_BASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_base"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_BASE="${CLICKHOUSE_DATABASE}.base_table"
TABLE_REPLACE="${CLICKHOUSE_DATABASE}.replace_table"
TABLE_FRESH="${CLICKHOUSE_DATABASE}.fresh_table"
REPLACE_LOG="${CLICKHOUSE_TMP}/04162_replace_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

# Defense-in-depth: always disable the failpoint and drop the test tables on exit, so an
# early failure (e.g. an unexpected throw before the cleanup tail) cannot leave the
# server-wide failpoint enabled and disrupt later tests on the same server. `SYSTEM
# DISABLE FAILPOINT` on an already-disabled failpoint is a no-op, and the DROPs use
# IF EXISTS, so running this twice (trap + normal tail) is safe.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_after_inner_create" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_BASE};" 2>/dev/null || true
    rm -f "${REPLACE_LOG}"
' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_BASE}; DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_FRESH};"

# Base object-storage disk, committed via a real table so the cache disk below has a
# backing disk to wrap.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_BASE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_BASE}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_BASE}_objstore/');"

# Enable the late-failure failpoint, then CREATE OR REPLACE TABLE t with a fresh inline
# cache disk N (settings S1, max_size='1Mi'). The inner create registers N under the
# caller-owned scope, then the failpoint throws before the rename. With the fix the scope
# rolls N's registration back.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_or_replace_fail_after_inner_create;"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE TABLE ${TABLE_REPLACE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '1Mi');" >"${REPLACE_LOG}" 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_after_inner_create;" 2>/dev/null

echo -n "create_or_replace_failed_with_injected_fault: "
grep -qE "FAULT_INJECTED" "${REPLACE_LOG}" && echo yes || echo no

# The disk name must be free now (no leaked tentative/committed entry). A fresh CREATE
# reusing the SAME name N but DIFFERENT settings S2 (max_size='2Mi') must succeed; if the
# bug had left N committed with S1, the settings-hash check would reject this with
# BAD_ARGUMENTS until restart. A different cache path avoids colliding on the dropped
# temporary table's still-held on-disk cache lock (see the Atomic delayed-drop note above).
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FRESH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data_v2/',
    max_size = '2Mi');
INSERT INTO ${TABLE_FRESH} VALUES (42);
SELECT count(), sum(a) FROM ${TABLE_FRESH};"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_BASE};"
rm -f "${REPLACE_LOG}"
