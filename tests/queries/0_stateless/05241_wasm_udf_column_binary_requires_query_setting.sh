#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` stays behind `allow_experimental_column_binary_format` when it is used as the
# `serialization_format` of a `BUFFERED_V1` WebAssembly UDF. Declaring the function does not need
# the setting, but every query that calls the function does.

MODULE="cb_gate_${CLICKHOUSE_DATABASE}"
FUNC="wasm_cb_gate_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"

cat "${CUR_DIR}/wasm/columnar_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 0 --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (s String) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';" && echo "declared without the setting"

${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 0 --query "
SELECT ${FUNC}(toString(number)) FROM numbers(3) FORMAT Null;" 2>&1 \
  | grep -o -m1 'SUPPORT_IS_DISABLED'

${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1 --query "
SELECT count(), min(${FUNC}(toString(number))) >= 1 FROM numbers(3);"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"
