#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARTITIONED_TABLE="t_iceberg_explain_partitioned_${CLICKHOUSE_DATABASE}_${RANDOM}"
UNPARTITIONED_TABLE="t_iceberg_explain_unpartitioned_${CLICKHOUSE_DATABASE}_${RANDOM}"
PARTITIONED_PATH="${USER_FILES_PATH}/${PARTITIONED_TABLE}/"
UNPARTITIONED_PATH="${USER_FILES_PATH}/${UNPARTITIONED_TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PARTITIONED_TABLE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${UNPARTITIONED_TABLE}"
    rm -rf "${PARTITIONED_PATH}" "${UNPARTITIONED_PATH}"
}
trap cleanup EXIT

normalize_explain()
{
    local query="$1"
    local output
    output=$(${CLICKHOUSE_CLIENT} --query "
        SELECT trimLeft(explain)
        FROM ($query)
        WHERE startsWith(trimLeft(explain), 'Partition')
           OR startsWith(trimLeft(explain), 'MinMax')
           OR startsWith(trimLeft(explain), 'Partitions:')
           OR startsWith(trimLeft(explain), 'Files:')
    ")

    if [[ -z "$output" ]]; then
        echo "NoIndexes"
    else
        printf '%s\n' "$output"
    fi
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PARTITIONED_TABLE} (part Int32, value Int32)
    ENGINE = IcebergLocal('${PARTITIONED_PATH}', 'Parquet')
    PARTITION BY (part)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${PARTITIONED_TABLE} VALUES (1, 10)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${PARTITIONED_TABLE} VALUES (2, 20)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${PARTITIONED_TABLE} VALUES (3, 30)"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${UNPARTITIONED_TABLE} (value Int32)
    ENGINE = IcebergLocal('${UNPARTITIONED_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${UNPARTITIONED_TABLE} VALUES (10)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${UNPARTITIONED_TABLE} VALUES (20)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${UNPARTITIONED_TABLE} VALUES (30)"

echo "combined"
normalize_explain "EXPLAIN indexes = 1 SELECT * FROM ${PARTITIONED_TABLE} WHERE part IN (2, 3) AND value = 30"

echo "minmax_only"
normalize_explain "EXPLAIN indexes = 1 SELECT * FROM ${UNPARTITIONED_TABLE} WHERE value = 20"

echo "disabled"
normalize_explain "EXPLAIN indexes = 1 SELECT * FROM ${PARTITIONED_TABLE} WHERE part IN (2, 3) AND value = 30 SETTINGS use_iceberg_partition_pruning = 0"
