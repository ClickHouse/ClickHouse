#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A WASM UDF takes the framework's default handling of `Nullable`, `Variant` and `Dynamic`
# arguments: a `Variant` or `Dynamic` value is dispatched to the declared type, and a null row
# answers `NULL` without reaching the guest. This pins that contract, which earlier revisions of
# this function overrode with an ABI-specific one.

MODULE="dyn_null_${CLICKHOUSE_DATABASE}"
FUNC="wasm_dyn_null_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_json'
    ARGUMENTS (s String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'JSONEachRow'"

echo "a Dynamic argument is dispatched to the declared type"
${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC}(CAST('abc', 'Dynamic'))"

echo "a Variant argument is dispatched the same way"
${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC}(CAST('abc', 'Variant(UInt8, String)'))"

echo "a null row answers NULL without reaching the guest"
${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC}(CAST(NULL, 'Dynamic'))"

echo "and a mixed block keeps one result per row"
${CLICKHOUSE_CLIENT} --query "
SELECT ${FUNC}(d) FROM (SELECT CAST(NULL, 'Dynamic') AS d UNION ALL SELECT CAST('abc', 'Dynamic')) ORDER BY d IS NULL DESC"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
