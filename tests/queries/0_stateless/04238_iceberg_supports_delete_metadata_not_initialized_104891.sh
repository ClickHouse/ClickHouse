#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104891.
#
# `DataLakeConfiguration::supportsDelete` and
# `DataLakeConfiguration::supportsParallelInsert` are reached by the query
# interpreters BEFORE the lazy-init in `checkMutationIsPossible` / `read` /
# `write`. On a freshly attached data lake table whose `current_metadata`
# has not been populated (e.g. because the previous `update` failed and was
# swallowed by the lazy-init catch in `StorageObjectStorage`'s constructor),
# the `assertInitialized` call inside these two methods raised
# `LOGICAL_ERROR: 'Metadata is not initialized'`, fatal in debug / sanitizer
# builds.
#
# This is the sibling issue of the one fixed for `OPTIMIZE` / `ALTER` by
# #104738 (test 04230). PR #104917 makes
# `StorageObjectStorage::supportsDelete` and
# `StorageObjectStorage::supportsParallelInsert` call
# `configuration->lazyInitializeIfNeeded(object_storage, ctx)` for data
# lake configurations, mirroring the existing pattern used by
# `supportedPrewhereColumns` and `getColumnSizes`.
#
# Each case below uses a fresh corrupted-metadata table so the operation
# is exercised against uninitialized metadata - any preceding operation
# that completed the lazy-init would mask the bug. The exact error code
# raised by each case depends on what `update` happens to fail with for
# the corrupted metadata file; we only care that it is NOT a logical
# error and that the server keeps running.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/test_104891_${CLICKHOUSE_DATABASE}"
trap "rm -rf '${BASE_DIR}' 2>/dev/null" EXIT
mkdir -p "${BASE_DIR}"

# `run_case` sets up a fresh table whose on-disk metadata is corrupted,
# DETACH+ATTACHes it to reset `current_metadata`, then runs `$2` against
# it and verifies the result is NOT a logical error. `__TABLE__` in `$2`
# is replaced with the per-case table name before execution.
run_case() {
    local case_name=$1
    local query=$2

    local table="t_${case_name}_${RANDOM}"
    local table_path="${BASE_DIR}/${table}/"

    # Step 1: create the table with the deflate metadata format.
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (c0 Int32)
        ENGINE = IcebergLocal('${table_path}')
        SETTINGS iceberg_metadata_compression_method = 'deflate'
    "

    # Step 2: corrupt the on-disk metadata via an INSERT with an unsupported
    # compression level. The INSERT itself must fail.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
                         --output_format_compression_level=11 \
        --query "INSERT INTO ${table} VALUES (2)" 2>&1 \
        | grep -F 'INCORRECT_DATA' > /dev/null \
        && echo "[${case_name}] INSERT failed as expected"

    # Step 3: DETACH+ATTACH to clear the in-memory `current_metadata` cache.
    # Silence the load-failure warning the client forwards to its stderr.
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table} SYNC"
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal \
        --query "ATTACH TABLE ${table}" 2>/dev/null

    # Step 4: run the case query against the freshly-attached table.
    local rendered=${query//__TABLE__/${table}}
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
                         --allow_experimental_iceberg_compaction=1 \
        --query "${rendered}" 2>&1 \
        | grep -F 'Logical error' > /dev/null \
        && echo "[${case_name}] FAIL: crashed with Logical error" \
        || echo "[${case_name}] did not crash with Logical error"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table} SYNC"
}

# `DELETE FROM` reaches `supportsDelete` - the primary fix path verified end
# to end: a corrupted-metadata table that we DETACH+ATTACH crashes the server
# without this PR's `supportsDelete` lazy-init guard (`Logical error: Metadata
# is not initialized`), and stays alive with it.
run_case delete       "DELETE FROM __TABLE__ WHERE c0 = 0"

# `TRUNCATE TABLE` - sibling shape of `DELETE FROM`. In the corrupted-metadata
# state used by this test, `TRUNCATE` already errors with `INCORRECT_DATA`
# before ever reaching the `supports*` methods, so the case below is included
# as shape coverage (it documents that the operation does not regress into a
# logical error path) rather than as a direct trigger of this PR's fix.
run_case truncate     "TRUNCATE TABLE __TABLE__"

# Lightweight `UPDATE` (`InterpreterUpdateQuery`). Like `TRUNCATE`, in the
# corrupted-metadata state used here the operation errors earlier than the
# `supportsDelete` call site, so this is shape coverage.
run_case update       "UPDATE __TABLE__ SET c0 = 1 WHERE c0 = 0"

# `INSERT INTO` is the other half of the fix (`supportsParallelInsert`).
# In the corrupted-metadata state used here, the INSERT pipeline triggers a
# metadata load that fails with `INCORRECT_DATA` before reaching
# `InsertDependenciesBuilder::supportsParallelInsert`, so this is shape
# coverage for the new method body rather than a direct crash repro.
run_case insert       "INSERT INTO __TABLE__ VALUES (3)"

# `ALTER TABLE ... DELETE` (mutation). Also covered by parent PR #104738
# (test 04230) but kept here for completeness against the corrupted-metadata
# state.
run_case alter_delete "ALTER TABLE __TABLE__ DELETE WHERE c0 = 0"

# `ALTER TABLE ... UPDATE` (mutation) - sibling to `ALTER DELETE`.
run_case alter_update "ALTER TABLE __TABLE__ UPDATE c0 = 1 WHERE c0 = 0"

# The server is still alive after all cases.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
