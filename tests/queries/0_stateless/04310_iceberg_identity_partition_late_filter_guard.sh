#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: toggles a process-global failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
POLICY="p_${CLICKHOUSE_DATABASE}_${RANDOM}"

trap "rm -rf \"${TABLE_PATH}\" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query \"DROP ROW POLICY IF EXISTS ${POLICY} ON ${TABLE}\" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query \"SYSTEM DISABLE FAILPOINT datalake_simulate_unresolved_prewhere_metadata\" 2>/dev/null" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64, region String, val Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY region
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1, 'East', 10), (2, 'West', 20), (3, 'East', 30)"

# region is an identity partition column, so it must not be filtered inside the reader: in a table
# whose data files omit it the value comes from the manifest only after the reader runs. ClickHouse
# writes it into the files, so this test asserts the contract, not the backfilled value.
echo "-- filter on the identity column returns its rows"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} WHERE region = 'East' ORDER BY id"

# The failpoint makes supportedPrewhereColumns answer as if the metadata were unresolved, which is
# the state a wrapper reaches when a child is cold or briefly unreadable during analysis. The
# contract then cannot name region, so the filter survives into the read step.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT datalake_simulate_unresolved_prewhere_metadata"

echo "-- an identity-partition filter that escaped the analysis-time contract is refused"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} PREWHERE region = 'East' ORDER BY id" 2>&1 \
    | grep -c "ILLEGAL_PREWHERE"

echo "-- a filter on a physical column is still allowed"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} PREWHERE val > 15 ORDER BY id"

# The guard covers row policies as well: a policy predicate is pushed to the same place a PREWHERE
# condition is, so it is refused for the same reason.
echo "-- a row policy on the identity column is refused too"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${POLICY} ON ${TABLE} USING region = 'East' AS PERMISSIVE TO ALL"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} ORDER BY id" 2>&1 \
    | grep -c "ILLEGAL_PREWHERE"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${POLICY} ON ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT datalake_simulate_unresolved_prewhere_metadata"

echo "-- the identity column is excluded again once the contract can name it"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} PREWHERE region = 'East' ORDER BY id" 2>&1 \
    | grep -c "ILLEGAL_PREWHERE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
