#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Cross-kind coercion must be rejected in BUFFERED_V1 RowBinary just as in ROW_DIRECT.
# Float ↔ Int cross-kind must throw ILLEGAL_TYPE_OF_ARGUMENT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_rb_i32;
DROP FUNCTION IF EXISTS wasm_rb_f32;
DROP FUNCTION IF EXISTS wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_rb_reject';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_rb_reject', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_rb_i32
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_rb_reject' :: 'identity_rowbinary_i32'
    ARGUMENTS (n Int32) RETURNS Int32
    SETTINGS serialization_format = 'RowBinary';

CREATE FUNCTION wasm_rb_f32
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_rb_reject' :: 'identity_rowbinary_f32'
    ARGUMENTS (n Float32) RETURNS Float32
    SETTINGS serialization_format = 'RowBinary';

CREATE FUNCTION wasm_rb_f64
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_rb_reject' :: 'identity_rowbinary_f64'
    ARGUMENTS (n Float64) RETURNS Float64
    SETTINGS serialization_format = 'RowBinary';

-- Float → Int cross-kind must be rejected.
SELECT wasm_rb_i32(toFloat64(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT wasm_rb_i32(toFloat32(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Int → Float cross-kind must be rejected.
SELECT wasm_rb_f32(toInt32(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT wasm_rb_f64(toInt32(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Float32 → Float64 cross-width must be rejected (different WASM kind).
SELECT wasm_rb_f64(toFloat32(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Correct types must work.
SELECT wasm_rb_i32(toInt32(42));
SELECT wasm_rb_f32(toFloat32(1.5));
SELECT wasm_rb_f64(toFloat64(2.5));

DROP FUNCTION wasm_rb_i32;
DROP FUNCTION wasm_rb_f32;
DROP FUNCTION wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_rb_reject';
EOF
