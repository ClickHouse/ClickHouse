#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"
CHECK_QUERY="SELECT count(), sum(id), sum(length(s)), min(id), max(id)"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 50000, output_format_parquet_compression_method = 'none'"

for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 \
        --query "INSERT INTO ${TABLE} SELECT number + ${i} * 300000, toString(cityHash64(number)) FROM numbers(300000)"
done

MANIFEST=$(find "${TABLE_PATH}metadata" -name '*.avro' ! -name 'snap-*' | head -1)
echo "--- split_offsets in the manifest"
${CLICKHOUSE_CLIENT} --query "SELECT length(data_file.split_offsets) FROM file('${MANIFEST}', 'Avro')"

echo "--- local"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${TABLE}"

run_cluster()
{
    local name=$1
    local settings=$2
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_${RANDOM}"
    echo "--- ${name}"
    ${CLICKHOUSE_CLIENT} --query_id="${query_id}" --max_threads=4 --query "${CHECK_QUERY} FROM ${CLUSTER_FN} SETTINGS ${settings}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(ProfileEvents['EngineFileLikeReadFiles'])
        FROM system.query_log
        WHERE initial_query_id = '${query_id}' AND type = 'QueryFinish' AND is_initial_query = 0"
}

run_cluster "file" "cluster_table_function_split_granularity = 'file'"
run_cluster "bucket_per_row_group" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0"
run_cluster "bucket_byte_ranges" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 1048576"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
