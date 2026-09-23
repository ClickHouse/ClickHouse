#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# A WASM UDF is never handed a nullable argument column: the framework strips the `Nullable`
# wrappers before the call and reapplies them to the result, exactly as it does for built-in
# functions. This is what the `NULL handling` section of the documentation describes, and it is
# only observable from SQL, so pin it here.

MODULE="null_prop_${CLICKHOUSE_DATABASE}"
FUNC="wasm_null_prop_${CLICKHOUSE_DATABASE}"
FUNC_NULLABLE="wasm_null_prop_nullable_${CLICKHOUSE_DATABASE}"
FUNC_ARRAY="wasm_null_prop_array_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DROP FUNCTION IF EXISTS ${FUNC_NULLABLE};
DROP FUNCTION IF EXISTS ${FUNC_ARRAY};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

# `str_byte_sum_col` sums the bytes of each string. Declared with a non-nullable signature.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'str_byte_sum_col'
    ARGUMENTS (s String) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';"

# The declared return type is not the type of the expression: a `Nullable` argument makes the
# result `Nullable` too, because the wrapper is reapplied on the way out.
${CLICKHOUSE_CLIENT} --query "
SELECT toTypeName(${FUNC}(s)) FROM (SELECT CAST(NULL, 'Nullable(String)') AS s)"

# The `NULL` row yields `NULL` without the module being consulted, while the empty string - the
# default value the module would see if nulls were substituted - yields 0. The two are therefore
# distinguishable from SQL even though the module cannot tell them apart.
${CLICKHOUSE_CLIENT} --query "
SELECT s, ${FUNC}(s) AS r
FROM (SELECT arrayJoin([CAST('ab', 'Nullable(String)'), NULL, '']) AS s)
ORDER BY s NULLS LAST" | sed 's/^\t/(empty)\t/'

# A return type that cannot be `Nullable` - `Array`, `Tuple`, `Map` - takes the other branch of
# the framework's null handling: there is no `NULL` to propagate, so the null rows are replaced by
# the default of the nested argument type and the module runs on those. The result type stays
# non-nullable and the `NULL` row gets the module's answer for the empty string, which is the
# same answer the empty string itself gets.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_ARRAY}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'array_of_len_col'
    ARGUMENTS (s String) RETURNS Array(UInt64)
    SETTINGS serialization_format = 'ColumnBinary';"

${CLICKHOUSE_CLIENT} --query "
SELECT toTypeName(${FUNC_ARRAY}(s)) FROM (SELECT CAST(NULL, 'Nullable(String)') AS s)"

${CLICKHOUSE_CLIENT} --query "
SELECT s, ${FUNC_ARRAY}(s) AS r
FROM (SELECT arrayJoin([CAST('ab', 'Nullable(String)'), NULL, '']) AS s)
ORDER BY s NULLS LAST" | sed 's/^\t/(empty)\t/'

# Declaring a `Nullable` return type does not hand the module nullable arguments; it only
# requires the module to return a nullable column. This one returns a plain `COL_FIXED64`, so the
# mismatch must be reported as bad data rather than as an internal error.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_NULLABLE}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'str_byte_sum_col'
    ARGUMENTS (s String) RETURNS Nullable(UInt64)
    SETTINGS serialization_format = 'ColumnBinary';"

${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC_NULLABLE}('ab')" 2>&1 \
    | grep -oE 'INCORRECT_DATA|LOGICAL_ERROR' | head -1

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DROP FUNCTION IF EXISTS ${FUNC_NULLABLE};
DROP FUNCTION IF EXISTS ${FUNC_ARRAY};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
