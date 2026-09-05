#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Dynamic` argument is dispatched to the declared argument type, and the call is then typed by
# what the definition declares in `RETURNS` rather than left as `Dynamic`. The framework carries
# the null a `Dynamic` row may hold in a `Nullable` of that type, so a result type that cannot be
# put inside `Nullable` keeps the `Dynamic` result instead of answering a default value for a null.

MODULE="dyn_result_type_${CLICKHOUSE_DATABASE}"
FUNC_NUMERIC="wasm_dyn_numeric_${CLICKHOUSE_DATABASE}"
FUNC_ARRAY="wasm_dyn_array_${CLICKHOUSE_DATABASE}"
FUNC_NULLABLE="wasm_dyn_nullable_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC_NUMERIC};
DROP FUNCTION IF EXISTS ${FUNC_ARRAY};
DROP FUNCTION IF EXISTS ${FUNC_NULLABLE};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_NUMERIC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (x Int8) RETURNS UInt32
    SETTINGS serialization_format = 'CSV'"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_ARRAY}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_json'
    ARGUMENTS (s String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'JSONEachRow'"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_NULLABLE}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (x Int8) RETURNS Nullable(UInt32)
    SETTINGS serialization_format = 'CSV'"

echo "a declared argument type is answered by the declared result type"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(${FUNC_NUMERIC}(1))"

echo "a Dynamic argument keeps that result type, made nullable for the null such a row may hold"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(${FUNC_NUMERIC}(CAST(1, 'Dynamic'))), ${FUNC_NUMERIC}(CAST(1, 'Dynamic'))"

echo "and a null row still answers NULL without reaching the guest"
${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC_NUMERIC}(CAST(NULL, 'Dynamic'))"

echo "a result type that cannot be inside Nullable keeps the Dynamic result, null included"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(${FUNC_ARRAY}(CAST('abc', 'Dynamic'))), ${FUNC_ARRAY}(CAST(NULL, 'Dynamic'))"

echo "a result type that is already Nullable carries the null itself and is not nested"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(${FUNC_NULLABLE}(CAST(1, 'Dynamic'))), ${FUNC_NULLABLE}(CAST(1, 'Dynamic')), ${FUNC_NULLABLE}(CAST(NULL, 'Dynamic'))"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC_NUMERIC};
DROP FUNCTION IF EXISTS ${FUNC_ARRAY};
DROP FUNCTION IF EXISTS ${FUNC_NULLABLE};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
