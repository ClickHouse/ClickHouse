#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-parallel: copies a data lake table into the shared user_files directory.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107334
# Reading a persistent DeltaLake table whose first access is through merge() reached
# DeltaLakeMetadataDeltaKernel::iterate() with a storage snapshot that had no pinned
# datalake_table_state and aborted the server with LOGICAL_ERROR
# 'No version found in table state snapshot'. merge() builds the child storage snapshot
# without updateExternalDynamicMetadataIfExists(), so the state must be pinned in
# StorageObjectStorage::read() instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta/"

trap "rm -rf \"${TABLE_PATH}\" 2>/dev/null" EXIT

mkdir -p "${TABLE_PATH}"
cp -r "${CUR_DIR}"/data_delta_lake/struct_column_mapping/* "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"

${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs=1 --query "
    CREATE TABLE t ENGINE = DeltaLakeLocal('${TABLE_PATH}')
"

# merge() is the SOLE first access to the table, so it never goes through the analyzer's
# updateExternalDynamicMetadataIfExists() that pins datalake_table_state. A non-trivial read
# (subcolumn, not count()) forces a real read pipeline into iterate(). Without the fix this
# aborts the server; with the fix read() pins the state first and the query returns rows.
${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs=1 --query "
    SELECT c1.id FROM merge(currentDatabase(), '^t\$') ORDER BY c1.id
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
