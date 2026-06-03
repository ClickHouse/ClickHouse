#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: toggles a process-global failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap "rm -rf \"${TABLE_PATH}\" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query \"SYSTEM DISABLE FAILPOINT datalake_simulate_missing_table_state\" 2>/dev/null" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

# ORDER BY makes the planner reach isDataSortedBySortingKey in addition to iterate.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    ORDER BY c0
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# The failpoint strips datalake_table_state from the snapshot reaching the read step,
# deterministically reproducing the concurrent-commit race. Without the fix each read
# throws LOGICAL_ERROR "Can't extract iceberg table state"; with the fix the read step
# re-fetches the state once for every pipeline consumer, so both queries succeed.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT datalake_simulate_missing_table_state"

# iterate path.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"
# isDataSortedBySortingKey / read-in-order path.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT datalake_simulate_missing_table_state"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
