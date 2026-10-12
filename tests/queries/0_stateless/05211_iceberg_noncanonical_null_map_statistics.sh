#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_null_map_bytes"
rm -rf "${TABLE_PATH}"

# `if` forwards its raw condition column as the null map, so the map here holds 0, 2 and 4: byte 0
# marks the 10 rows holding 'x' and the 2s and 4s are the 20 rows that are equally NULL.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1;
    CREATE TABLE t_null_map_bytes_iceberg (id UInt64, e Nullable(String))
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO t_null_map_bytes_iceberg SELECT number, if(toUInt8((number % 3) * 2), NULL, 'x') FROM numbers(30);
"

echo '--- a null count is the number of NULL rows, never the sum of the null-map bytes ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        sum(record_count) AS rows,
        sum(null_value_counts[2]) AS nulls,
        countIf(null_value_counts[2] > record_count) AS impossible_entries
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't_null_map_bytes_iceberg' AND content = 0;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_null_map_bytes_iceberg;"
rm -rf "${TABLE_PATH}"
