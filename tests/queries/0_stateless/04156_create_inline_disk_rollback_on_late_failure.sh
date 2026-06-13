#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the CREATE / ATTACH leak that clickhouse-gh[bot] flagged on
# PR #103818 (block thread on src/Interpreters/Context.cpp:6443, 2026-06-09T14:42Z).
#
# Before the fix, an unscoped CREATE / ATTACH that registered a fresh inline custom
# disk committed the registration the moment the disk-settings validation passed
# (`commitUnscopedDiskObservation` in DiskFromAST.cpp), which is BEFORE the outer
# CREATE metadata transition is durable. If a later step in
# `InterpreterCreateQuery::doCreateTable` threw (`validateStorage`, the replicated
# create fault injection, `database->createTable` / `writeMetadataFile`), the disk
# stayed registered in `DiskSelector` / `FileCacheFactory` with no table owning it.
# A later valid `CREATE TABLE ... SETTINGS disk = disk(name = X, <different settings>)`
# was then rejected by the settings-hash check until server restart.
#
# The fix gives CREATE / ATTACH a caller-owned scope (an ambient
# `CustomDiskRegistrationScope` installed by `InterpreterCreateQuery`) that commits
# only AFTER the table metadata is durable, mirroring the ALTER path. A failure
# before the commit rolls the freshly-registered disk back.
#
# This test drives the late failure deterministically with a ONCE failpoint
# `create_table_fail_after_disk_registration_before_metadata`, which throws inside
# `doCreateTable` right after `validateStorage` (disk already registered) and before
# `database->createTable`.
#
# Sequence:
#   1. Create a base object-storage disk via a committed table.
#   2. Enable the failpoint. CREATE t1 with a fresh inline cache disk X (settings S1).
#      The disk is registered, then the failpoint throws. With the fix the ambient
#      scope rolls X back; without the fix X leaks committed with settings S1.
#   3. Disable the failpoint. CREATE t2 with the SAME disk name X but DIFFERENT
#      settings S2 (different max_size). With the fix X was rolled back, so this is a
#      fresh registration and succeeds. Without the fix the leaked S1 entry's
#      settings-hash rejects S2 with BAD_ARGUMENTS.
#
# Verifies the fix by asserting t1 failed with the injected fault, and t2 (same name,
# different settings) was created and is queryable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_BASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_base"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_BASE="${CLICKHOUSE_DATABASE}.base_table"
TABLE_FAIL="${CLICKHOUSE_DATABASE}.fail_table"
TABLE_FRESH="${CLICKHOUSE_DATABASE}.fresh_table"
FAIL_LOG="${CLICKHOUSE_TMP}/04156_fail_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_BASE}; DROP TABLE IF EXISTS ${TABLE_FAIL}; DROP TABLE IF EXISTS ${TABLE_FRESH};"

# Base object-storage disk, committed via a real table so the cache disk below has a
# backing disk to wrap.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_BASE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_BASE}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_BASE}_objstore/');"

# Enable the late-failure failpoint, then CREATE t1 with a fresh inline cache disk X
# (settings S1, max_size='1Mi'). The disk is registered, then the failpoint throws
# after validateStorage. With the fix the ambient scope rolls X back.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_table_fail_after_disk_registration_before_metadata;"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FAIL} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '1Mi');" >"${FAIL_LOG}" 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_table_fail_after_disk_registration_before_metadata;" 2>/dev/null

echo -n "create_failed_with_injected_fault: "
grep -qE "FAULT_INJECTED" "${FAIL_LOG}" && echo yes || echo no

# The disk name must be free now (no leaked tentative/committed entry). A fresh CREATE
# with the SAME name X but DIFFERENT settings S2 (max_size='2Mi') must succeed; if the
# bug had left X committed with S1, the settings-hash check would reject this with
# BAD_ARGUMENTS until restart.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_FRESH} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');
INSERT INTO ${TABLE_FRESH} VALUES (42);
SELECT count(), sum(a) FROM ${TABLE_FRESH};"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_FRESH}; DROP TABLE IF EXISTS ${TABLE_FAIL}; DROP TABLE IF EXISTS ${TABLE_BASE};"
rm -f "${FAIL_LOG}"
