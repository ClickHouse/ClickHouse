#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-parallel: toggles a process-global failpoint.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107334
# DeltaLakeMetadataDeltaKernel::iterate() throws LOGICAL_ERROR
# 'No version found in table state snapshot' when the storage snapshot reaching the read
# step has no pinned datalake_table_state (e.g. a first access through merge() or a cluster
# function, which builds the child snapshot without updateExternalDynamicMetadataIfExists()).
# The DeltaLake counterpart of 04305_iceberg_missing_table_state.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap "rm -rf \"${TABLE_PATH}\" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query \"SYSTEM DISABLE FAILPOINT datalake_simulate_missing_table_state\" 2>/dev/null" EXIT

mkdir -p "${TABLE_PATH}"
cp -r "${CUR_DIR}"/data_delta_lake/struct_column_mapping/* "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs=1 --query "
    CREATE TABLE ${TABLE} ENGINE = DeltaLakeLocal('${TABLE_PATH}')
"

# The failpoint strips datalake_table_state from the snapshot reaching the read step,
# deterministically reproducing the missing-state path that merge()/cluster reads hit.
# Without the fix the read throws "No version found in table state snapshot" in
# DeltaLakeMetadataDeltaKernel::iterate(); with the fix read() pins one consistent snapshot
# before reading, so the query succeeds. The failpoint exists only in this build, so on the
# unfixed master binary SYSTEM ENABLE FAILPOINT fails cleanly and the read is unaffected.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT datalake_simulate_missing_table_state"

# Non-trivial read (subcolumn, not count) so a real read pipeline runs into iterate().
${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs=1 \
    --query "SELECT c1.id FROM ${TABLE} ORDER BY c1.id"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT datalake_simulate_missing_table_state"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
