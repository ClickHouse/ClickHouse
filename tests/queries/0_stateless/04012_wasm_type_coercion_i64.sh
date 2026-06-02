#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Int64 coercions in ROW_DIRECT and BUFFERED_V1 RowBinary.
# UInt64 → Int64 is same i64 WASM kind → accepted.
# Int32 and smaller → i32 WASM kind → rejected for Int64 parameter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_raw_i64;
DROP FUNCTION IF EXISTS wasm_rb_i64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_i64';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_i64', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_raw_i64
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'identity_int_i64' :: 'identity_raw_i64'
    ARGUMENTS (x Int64) RETURNS Int64;

CREATE FUNCTION wasm_rb_i64
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_i64' :: 'identity_rowbinary_i64'
    ARGUMENTS (x Int64) RETURNS Int64
    SETTINGS serialization_format = 'RowBinary';

-- ROW_DIRECT: exact declared type.
SELECT wasm_raw_i64(toInt64(9223372036854775807));
SELECT wasm_raw_i64(toInt64(-9223372036854775808));
SELECT wasm_raw_i64(toInt64(0));

-- ROW_DIRECT: UInt64 → Int64 accepted (same i64 WASM kind).
SELECT wasm_raw_i64(toUInt64(42));

-- ROW_DIRECT: Int32 → Int64 rejected (i32 ≠ i64 WASM kind).
SELECT wasm_raw_i64(toInt32(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- ROW_DIRECT: small ints → i32 WASM kind → rejected for i64 parameter.
SELECT wasm_raw_i64(toInt8(1));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- ROW_DIRECT: Float → Int64 cross-kind rejected.
SELECT wasm_raw_i64(toFloat64(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- BUFFERED_V1 RowBinary: exact declared type.
SELECT wasm_rb_i64(toInt64(9223372036854775807));
SELECT wasm_rb_i64(toInt64(-9223372036854775808));
SELECT wasm_rb_i64(toInt64(0));

-- BUFFERED_V1: UInt64 → Int64: getArgumentsBlock() casts UInt64 column to Int64 before 8-byte LE serialization.
SELECT wasm_rb_i64(toUInt64(42));

-- BUFFERED_V1: Int32 → Int64 rejected (i32 ≠ i64).
SELECT wasm_rb_i64(toInt32(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Multiple rows.
SELECT wasm_rb_i64(toInt64(number)) FROM numbers(4);

DROP FUNCTION wasm_raw_i64;
DROP FUNCTION wasm_rb_i64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_i64';
EOF
