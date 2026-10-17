#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# Tag no-fasttest: requires IcebergLocal engine (USE_AVRO build option).
# Tag no-random-settings: depends on Parquet push-down defaults.
# Verify that IN-subquery returns correct results on IcebergLocal tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_iceberg_in_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap "rm -rf '${TABLE_PATH}'" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt64, payload String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --output_format_parquet_row_group_size=20000 \
    --output_format_parquet_data_page_size=4096 \
    --output_format_parquet_write_page_index=1 \
    --output_format_parquet_write_bloom_filter=1 \
    --output_format_parquet_use_custom_encoder=1 \
    --max_threads=1 \
    --query "
INSERT INTO ${TABLE}
SELECT number AS id, repeat('x', 16) AS payload
FROM numbers(200000)
ORDER BY id
"

# Correct results with literal IN.
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${TABLE}
WHERE id IN (100, 100000, 199900)
"

# Correct results with IN-subquery.
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${TABLE}
WHERE id IN (SELECT arrayJoin([100, 100000, 199900])::Int64)
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
