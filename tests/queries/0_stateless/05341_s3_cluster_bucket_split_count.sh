#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 (MinIO)

# `SELECT count()` over Parquet files split into row group buckets must count the rows of the
# bucket, not of the whole file, for every task.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=10000 --output_format_parquet_row_group_size_bytes=100000 --max_insert_threads=1 \
    --query "INSERT INTO FUNCTION s3('${FILE}', 'test', 'testtest', 'Parquet') SELECT number AS id, toString(number) AS s FROM numbers(100000) SETTINGS s3_truncate_on_insert = 1"

echo "--- row groups"
${CLICKHOUSE_CLIENT} --query "SELECT num_row_groups > 1 FROM s3('${FILE}', 'test', 'testtest', 'ParquetMetadata')"

for granularity in file bucket; do
    echo "--- ${granularity}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', '${FILE}', 'test', 'testtest', 'Parquet') SETTINGS cluster_table_function_split_granularity = '${granularity}', cluster_table_function_buckets_batch_size = 0"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM s3Cluster('test_cluster_two_shards_localhost', '${FILE}', 'test', 'testtest', 'Parquet') SETTINGS cluster_table_function_split_granularity = '${granularity}', cluster_table_function_buckets_batch_size = 0"
done
