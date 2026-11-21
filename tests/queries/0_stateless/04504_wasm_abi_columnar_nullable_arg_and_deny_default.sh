#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS array_of_len;
DROP FUNCTION IF EXISTS bytes_equal_nullable;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_nullable_arg';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_nullable_arg', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

-- A declared Nullable(String) parameter turns the framework's NULL handling off, so the
-- module receives the null map. array_of_len_col returns [] for a NULL row and
-- [byte_len(s)] otherwise, which tells a NULL that reached the module apart from the ''
-- the framework would have substituted for it.
CREATE OR REPLACE FUNCTION array_of_len
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s Nullable(String)) RETURNS Array(UInt64)
    DETERMINISTIC;

SELECT s, array_of_len(s)
FROM (SELECT arrayJoin([CAST('abc', 'Nullable(String)'), NULL, '']) AS s)
ORDER BY s NULLS LAST;

-- With NULL handling off, a Nullable column also reaches a parameter declared as plain
-- String. executeColumnar must cast it to Nullable(String), not String, before
-- serialization, or the cast itself throws CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN on the
-- first NULL row.
CREATE OR REPLACE FUNCTION bytes_equal_nullable
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'bytes_equal_col'
    ARGUMENTS (a Nullable(String), b String) RETURNS UInt8
    DETERMINISTIC;

SELECT b, bytes_equal_nullable('abc', b)
FROM (SELECT arrayJoin([CAST('abc', 'Nullable(String)'), NULL]) AS b)
ORDER BY b NULLS LAST;
EOF

# validateColumnarWireSupportedType must deny-by-default: any type family it doesn't
# explicitly know how to encode (Dynamic, JSON/Object, ...) must be rejected when the
# function is declared rather than silently falling through as "supported" and failing
# deep inside buildColDescriptor once the first block is actually serialized.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION array_of_len_dynamic
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s Dynamic) RETURNS Array(UInt64)" 2>&1 \
    | grep -q "type is not supported" && echo "type is not supported"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION array_of_len_json
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s JSON) RETURNS Array(UInt64)" 2>&1 \
    | grep -q "type is not supported" && echo "type is not supported"

# An empty Tuple has no element to decode the rows from, so it is rejected up front too,
# at the top level and nested.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION empty_tuple_result
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s String) RETURNS Tuple()" 2>&1 \
    | grep -q "type is not supported" && echo "type is not supported"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION empty_tuple_result
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s String) RETURNS Array(Tuple())" 2>&1 \
    | grep -q "type is not supported" && echo "type is not supported"

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS array_of_len;
DROP FUNCTION IF EXISTS bytes_equal_nullable;
DROP FUNCTION IF EXISTS empty_tuple_result;
DROP FUNCTION IF EXISTS array_of_len_dynamic;
DROP FUNCTION IF EXISTS array_of_len_json;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_nullable_arg';
EOF
