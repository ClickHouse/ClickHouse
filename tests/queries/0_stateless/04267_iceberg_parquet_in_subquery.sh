#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# Tag no-fasttest: requires IcebergLocal engine (USE_AVRO build option).
# Tag no-random-settings: depends on Parquet push-down defaults.
# Iceberg with Parquet data files must eagerly build IN-subquery sets.

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

# Force small row groups (20k rows), small data pages (4k), page index and
# bloom filters for granular pruning.
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

# Correct results with IN-subquery.
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${TABLE}
WHERE id IN (SELECT arrayJoin([100, 100000, 199900])::UInt64)
"

# Verify actual pushdown via read_rows: should read far less than 200k.
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${TABLE}
WHERE id IN (SELECT arrayJoin([100, 100000, 199900])::UInt64)
SETTINGS log_comment = '100743_iceberg_subquery'
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
SELECT 'iceberg_pushed_down',
    read_rows < 50000 AS pushed_down
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '100743_iceberg_subquery'
ORDER BY event_time DESC
LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
