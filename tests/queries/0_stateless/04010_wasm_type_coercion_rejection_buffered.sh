#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Illegal coercions must be rejected in BUFFERED_V1 RowBinary:
#   Float→Int and Float64→Float32 must throw ILLEGAL_TYPE_OF_ARGUMENT.
# (Int→Float and Int32→Int64 widening coercions are allowed and tested separately.)

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

-- Float → Int must be rejected (narrowing/cross-kind).
SELECT wasm_rb_i32(toFloat64(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT wasm_rb_i32(toFloat32(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Float64 → Float32 narrowing must be rejected.
SELECT wasm_rb_f32(toFloat64(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Int64 → Int32 narrowing must be rejected.
SELECT wasm_rb_i32(toInt64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Correct types must work.
SELECT wasm_rb_i32(toInt32(42));
SELECT wasm_rb_f32(toFloat32(1.5));
SELECT wasm_rb_f64(toFloat64(2.5));

-- Int → Float widening is allowed.
SELECT wasm_rb_f32(toInt32(7));
SELECT wasm_rb_f64(toInt32(7));

-- Float32 → Float64 widening is allowed.
SELECT wasm_rb_f64(toFloat32(1.5));

DROP FUNCTION wasm_rb_i32;
DROP FUNCTION wasm_rb_f32;
DROP FUNCTION wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_rb_reject';
EOF
