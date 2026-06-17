#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression test for the clickhouse-gh[bot] review on PR #103818 (issue #63019),
# InterpreterCreateQuery.cpp catch path of doCreateOrReplaceTable (2026-06-17T09:11Z).
#
# CREATE OR REPLACE TABLE runs through doCreateOrReplaceTable, which creates a randomly-named
# temporary table via the inner doCreateTable (making its metadata durable and registering any
# inline custom disk under the caller-owned CustomDiskRegistrationScope), then runs the fill and
# the final rename/exchange. If a step before the rename throws, the catch path drops the leftover
# temporary table while the scope is still uncommitted, so the scope destructor rolls the inline
# disk back. That is correct ONLY when the cleanup DROP actually removes the temporary table.
#
# The gap: the cleanup DROP can itself throw. The catch then logged and kept rethrowing while the
# scope was still uncommitted, so ~CustomDiskRegistrationScope rolled the inline disk out of
# DiskSelector / FileCacheFactory even though the temporary table's durable metadata survived and
# still referenced that disk. The leftover table then pointed at a disk no longer registered.
#
# The fix commits the scope (keeps the disk) when the cleanup DROP fails, so the disk stays
# registered while a durable table still references it; a later DROP of the leftover table releases
# it through the normal path.
#
# This drives both failures deterministically with two ONCE failpoints:
#   create_or_replace_fail_after_inner_create - throws after the inner create, before the rename.
#   create_or_replace_fail_cleanup_drop       - throws inside the catch path's cleanup DROP.
#
# Sequence:
#   1. Create a base object-storage disk via a committed table.
#   2. Enable both failpoints. CREATE OR REPLACE TABLE with a fresh inline cache disk N. The inner
#      create registers N and makes the temporary table durable; the first failpoint throws; the
#      catch's cleanup DROP hits the second failpoint and throws. With the fix the scope is
#      committed, so N stays registered.
#   3. The leftover temporary table is still durable and references N. Assert N is still present in
#      system.disks (not rolled back) and the leftover table is queryable (its disk resolves).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_BASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_base"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
TABLE_BASE="${CLICKHOUSE_DATABASE}.base_table"
TABLE_REPLACE="${CLICKHOUSE_DATABASE}.replace_table"
REPLACE_LOG="${CLICKHOUSE_TMP}/04163_replace_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

# Defense-in-depth: always disable both failpoints and drop the test tables (including any leftover
# temporary table) on exit, so an early failure cannot leave a server-wide failpoint enabled or a
# durable temporary table behind to disrupt later tests on the same server. SYSTEM DISABLE FAILPOINT
# on an already-disabled failpoint is a no-op and the DROPs use IF EXISTS, so running this alongside
# the normal tail is safe.
trap '
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_after_inner_create" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_cleanup_drop" 2>/dev/null || true
    for t in $(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '\\_tmp\\_replace\\_%'" 2>/dev/null); do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.\`${t}\` SYNC" 2>/dev/null || true
    done
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_BASE};" 2>/dev/null || true
    rm -f "${REPLACE_LOG}"
' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_BASE};"

# Base object-storage disk, committed via a real table so the cache disk below has a backing disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_BASE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_BASE}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_BASE}_objstore/');"

# Enable both failpoints, then CREATE OR REPLACE TABLE with a fresh inline cache disk N. The inner
# create registers N and makes the temporary table durable; the first failpoint throws before the
# rename; the catch's cleanup DROP hits the second failpoint and throws. With the fix the scope is
# committed, so N stays registered while the durable temporary table still references it.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_or_replace_fail_after_inner_create;"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT create_or_replace_fail_cleanup_drop;"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE TABLE ${TABLE_REPLACE} (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '1Mi');" >"${REPLACE_LOG}" 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_after_inner_create;" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT create_or_replace_fail_cleanup_drop;" 2>/dev/null

echo -n "create_or_replace_failed_with_injected_fault: "
grep -qE "FAULT_INJECTED" "${REPLACE_LOG}" && echo yes || echo no

# The cleanup DROP failed, so the temporary table is still durable and references N. With the fix N
# was kept (committed), so it is still present in system.disks and the leftover table resolves its
# disk. Without the fix N was rolled back, so it is absent here.
echo -n "disk_kept_after_failed_cleanup: "
${CLICKHOUSE_CLIENT} --query "SELECT count() = 1 FROM system.disks WHERE name = '${SHARED_NAME}'"

# The leftover temporary table references N. Selecting from it must succeed (its disk resolves). If N
# had been rolled out from under the durable metadata, this would fail to find the disk.
LEFTOVER=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '\\_tmp\\_replace\\_%' LIMIT 1")
echo -n "leftover_temp_table_queryable: "
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.\`${LEFTOVER}\`"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.\`${LEFTOVER}\` SYNC" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_REPLACE}; DROP TABLE IF EXISTS ${TABLE_BASE};"
rm -f "${REPLACE_LOG}"
