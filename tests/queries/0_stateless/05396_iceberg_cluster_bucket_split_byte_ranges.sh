#!/usr/bin/env bash
# Tags: no-fasttest

# Cluster reads of an Iceberg table with `cluster_table_function_split_granularity = 'bucket'`:
# files are cut into byte ranges from the manifest (no footer reads on the initiator), the
# pieces of a file keep the Iceberg metadata of the file, so position deletes are applied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"
CHECK_QUERY="SELECT count(), sum(id), sum(length(s)), min(id), max(id)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# The format settings of the data files are taken from the table, not from the INSERT query.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 50000, output_format_parquet_compression_method = 'none'
"

# Three data files of several megabytes, each with several row groups.
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 \
        --query "INSERT INTO ${TABLE} SELECT number + ${i} * 300000, toString(cityHash64(number)) FROM numbers(300000)"
done

# Position deletes referencing every data file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE id % 7 = 0"

echo "--- local"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${TABLE}"

# Runs the check query on the cluster and tells whether the workers received more tasks than there
# are data files, i.e. whether the files were split. The exact number of byte ranges depends on the
# file sizes, so it is not printed.
run_cluster()
{
    local name=$1
    local settings=$2
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_${RANDOM}"
    echo "--- ${name}"
    ${CLICKHOUSE_CLIENT} --query_id="${query_id}" --max_threads=4 --query "${CHECK_QUERY} FROM ${CLUSTER_FN} SETTINGS ${settings}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT if(sum(ProfileEvents['EngineFileLikeReadFiles']) > 3, 'split', 'one task per file')
        FROM system.query_log
        WHERE initial_query_id = '${query_id}' AND type = 'QueryFinish' AND is_initial_query = 0"
}

run_cluster "file" "cluster_table_function_split_granularity = 'file'"
run_cluster "bucket_byte_ranges" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 1048576"
run_cluster "bucket_per_row_group" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0"
run_cluster "bucket_bitmap_deletes" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 1048576, use_roaring_bitmap_iceberg_positional_deletes = 1"
run_cluster "auto_few_files" "cluster_table_function_split_granularity = 'auto', cluster_table_function_buckets_batch_size = 1048576"
# The bucket size is an upper bound: three files are still split to give work to every reading thread.
run_cluster "bucket_larger_than_files" "cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 1073741824"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
