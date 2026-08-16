#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="$CLICKHOUSE_TEST_UNIQUE_NAME.parquet"
SCHEMA='t Tuple(a Tuple(j JSON(k UInt64)))'

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('$DATA_FILE', Parquet)
    SELECT CAST(tuple(tuple(CAST('{\"k\":1}' AS JSON(k UInt64)))) AS '$SCHEMA') AS t
    UNION ALL
    SELECT CAST(tuple(tuple(CAST('{\"k\":2}' AS JSON(k UInt64)))) AS '$SCHEMA') AS t
" --output_format_parquet_use_custom_encoder=1 --output_format_parquet_json_as_variant=1

${CLICKHOUSE_CLIENT} --query "
    SELECT t.a.j.k
    FROM file('$DATA_FILE', parquet, '$SCHEMA')
    PREWHERE t.a.j.k = 1
    FORMAT TSVRaw
" --input_format_parquet_use_native_reader_v3=1
