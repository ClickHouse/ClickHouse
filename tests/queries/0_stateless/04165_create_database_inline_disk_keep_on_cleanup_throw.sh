#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the clickhouse-gh[bot] review on PR #103818 (issue #63019),
# InterpreterCreateQuery.cpp catch path of createDatabase (2026-06-17T11:02Z).
#
# CREATE DATABASE ... SETTINGS disk = disk(...) registers the inline custom metadata disk under
# the ambient CustomDiskRegistrationScope. createDatabase attaches the database, makes the
# top-level metadata .sql file durable (`renamed = true`, its content references the inline disk),
# then loads/starts the database's tables. If a step after the .sql is durable throws, the catch
# path cleans up: it removes the metadata file and detaches the database while the scope is still
# uncommitted, so the scope destructor rolls the inline disk back. That is correct ONLY when the
# cleanup actually removes the durable metadata and detaches the database.
#
# The gap: the cleanup itself can throw (removeFileIfExists or detachDatabase). The catch then kept
# rethrowing while the scope was still uncommitted, so ~CustomDiskRegistrationScope rolled the
# inline disk out of DiskSelector / FileCacheFactory even though the durable metadata .sql (and the
# still-attached database) kept referencing that disk. A restart would then fail to load the .sql
# because the disk it names is gone.
#
# The fix commits the scope (keeps the disk) when the cleanup fails, so the disk stays registered
# while durable metadata / an attached database still reference it; a later DROP DATABASE releases
# it through the normal path.
#
# This drives both failures deterministically with two ONCE failpoints:
#   create_database_fail_after_metadata - throws after the metadata file is durable, before commit.
#   create_database_fail_cleanup        - throws inside the catch path's metadata-file removal.
#
# Sequence:
#   1. Enable both failpoints. CREATE DATABASE with a fresh inline object-storage metadata disk N. The
#      database is attached and the metadata .sql made durable (referencing N); the first failpoint
#      throws; the catch's cleanup hits the second failpoint and throws. With the fix the scope is
#      committed, so N stays registered.
#   2. Assert N is still present in system.disks (not rolled back).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_dbmeta"
DB_INLINE="${CLICKHOUSE_DATABASE}_inline"
CREATE_LOG="${CLICKHOUSE_TMP}/04165_create_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

# Defense-in-depth: always disable both failpoints and drop the test database on exit, so an early
# failure cannot leave a server-wide failpoint enabled or a durable database behind to disrupt later
# tests on the same server. SYSTEM DISABLE FAILPOINT on an already-disabled failpoint is a no-op and
# the DROP uses IF EXISTS, so running this alongside the normal tail is safe.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_database_fail_after_metadata" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_database_fail_cleanup" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_INLINE} SYNC" 2>/dev/null || true
    rm -f "${CREATE_LOG}"
' EXIT

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_INLINE} SYNC"

# Enable both failpoints, then CREATE DATABASE with a fresh inline object-storage metadata disk N. The
# database is attached and its metadata .sql made durable (referencing N); the first failpoint
# throws before the scope commit; the catch's metadata-file removal hits the second failpoint and
# throws. With the fix the scope is committed, so N stays registered while the durable metadata
# still references it.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_database_fail_after_metadata;"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_database_fail_cleanup;"

${CLICKHOUSE_CLIENT} --query "
CREATE DATABASE ${DB_INLINE} ENGINE = Atomic
SETTINGS disk = disk(
    name = '${DISK_NAME}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_NAME}_store/');" >"${CREATE_LOG}" 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_database_fail_after_metadata;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_database_fail_cleanup;" 2>/dev/null

echo -n "create_database_failed_with_injected_fault: "
grep -qE "FAULT_INJECTED" "${CREATE_LOG}" && echo yes || echo no

# The cleanup failed, so the database metadata is still durable and references N. With the fix N was
# kept (committed), so it is still present in system.disks. Without the fix N was rolled back, so it
# is absent here.
echo -n "disk_kept_after_failed_cleanup: "
${CLICKHOUSE_CLIENT} --query "SELECT count() = 1 FROM system.disks WHERE name = '${DISK_NAME}'"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_INLINE} SYNC" 2>/dev/null
rm -f "${CREATE_LOG}"
