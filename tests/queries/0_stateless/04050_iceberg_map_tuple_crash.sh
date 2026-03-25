#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/85955
# Reading an Iceberg table with Map(Tuple(Int), Tuple(Int)) throws
# std::out_of_range in unordered_map::at during Parquet column name remapping.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_map_tuple"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET input_format_parquet_use_native_reader_v3 = 1; -- Arrow-based reader has a known limitation with Map(Tuple, Tuple) remapping in Iceberg
    CREATE TABLE t_map_tuple (c0 Map(Tuple(Int32), Tuple(Int32))) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_map_tuple SELECT c0 FROM generateRandom('c0 Map(Tuple(Int32), Tuple(Int32))', 42, 1, 1) LIMIT 1;
    SELECT count() FROM t_map_tuple;
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_map_tuple"
rm -rf "${ICEBERG_TABLE_PATH}"
