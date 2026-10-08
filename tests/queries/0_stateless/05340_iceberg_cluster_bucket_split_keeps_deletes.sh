#!/usr/bin/env bash
# Tags: no-fasttest

# With `cluster_table_function_split_granularity = 'bucket'` the pieces of an Iceberg data file sent
# to the workers must keep the Iceberg metadata of the file, in particular its position delete files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"
CHECK_QUERY="SELECT count(), sum(id), min(id), max(id)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# The format settings of the data files are taken from the table, not from the INSERT query.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 50000
"

# Three data files with several row groups each.
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 \
        --query "INSERT INTO ${TABLE} SELECT number + ${i} * 300000, toString(number) FROM numbers(300000)"
done

# Position deletes referencing every data file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE id % 7 = 0"

echo "--- local"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${TABLE}"
echo "--- cluster, whole files"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${CLUSTER_FN} SETTINGS cluster_table_function_split_granularity = 'file'"
echo "--- cluster, a task per row group"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${CLUSTER_FN} SETTINGS cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0"
echo "--- cluster, a task per row group, bitmap deletes"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${CLUSTER_FN} SETTINGS cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0, use_roaring_bitmap_iceberg_positional_deletes = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
