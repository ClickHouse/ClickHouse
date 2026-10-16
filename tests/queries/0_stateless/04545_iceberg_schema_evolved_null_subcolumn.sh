#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel-replicas: `StorageObjectStorageCluster` does not delegate to the
#   underlying configuration for schema-evolution subcolumn handling; see
#   04147_iceberg_orc_schema_evolution_row_policy.sh for the same restriction.
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104856.
#
# When an Iceberg column added by `ALTER TABLE ... ADD COLUMN` is absent from
# older data files, IcebergSchemaProcessor::getSchemaTransformationDag
# synthesizes it as a constant default node in the schema-transformation
# ActionsDAG. Reading the `.null` subcolumn of that column (directly, or via
# `optimize_functions_to_subcolumns`) then extracts a subcolumn from a
# ColumnConst, which used to fail: `Bad cast from ColumnConst to ColumnNullable`
# in debug/sanitizer builds, `NOT_FOUND_COLUMN_IN_BLOCK` in release.
#
# The fix materializes the synthesized default node at its source, so the
# column leaves the transform as a full column like every other missing-column
# path. The ORC data files below force the strip-and-replay schema-transform
# path (the native Parquet reader fills defaults itself and does not exercise
# this code).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_evol_null"
TEST_TABLE="t_ice_evol_null"

rm -rf "${ICEBERG_PATH}"

# Create an Iceberg table and populate it with an ORC data file (no `e` column).
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE} (a Int64, b String)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'a Int64, b String')
        SELECT number, toString(number) FROM numbers(5);
"

# Evolve the schema: add a nullable column. Old rows have no value for it, so
# reads synthesize it as a constant NULL and go through the schema-transform.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TEST_TABLE} ADD COLUMN e Nullable(String);
"

# 1) Read the `.null` subcolumn directly. Every old row is NULL, so `.null` is 1.
${CLICKHOUSE_CLIENT} --query "
    SELECT e.null FROM ${TEST_TABLE} ORDER BY a
"

# 2) Aggregate over the `.null` subcolumn.
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(e.null) FROM ${TEST_TABLE}
"

# 3) The natural `IS NULL` / `IS NOT NULL` predicates. Pin
#    optimize_functions_to_subcolumns=1 (the runner randomizes it) so the
#    rewrite to the `.null` subcolumn is always exercised.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TEST_TABLE} WHERE e IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
    SELECT count() FROM ${TEST_TABLE} WHERE e IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
