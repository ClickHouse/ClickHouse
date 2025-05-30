#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file(03525_enum.parquet, Parquet, 'animal Enum8(\'dog\' = 1, \'cat\' = 2)') SETTINGS output_format_parquet_enum_as_byte_array=1, engine_file_truncate_on_insert=1 VALUES ('dog'), ('cat');"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('03525_enum.parquet', 'ParquetMetadata') FORMAT JSONCompactEachRow" | jq -r '.[7][] | select(.name == "animal") | "physical_type: \(.physical_type) logical_type: \(.logical_type)"'
