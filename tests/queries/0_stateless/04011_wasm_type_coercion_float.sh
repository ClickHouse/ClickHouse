#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Float coercions in ROW_DIRECT and BUFFERED_V1 RowBinary.
# Float32 → Float32, Float64 → Float64 (same kind) and Float32 → Float64 (widening) must work.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_raw_f32;
DROP FUNCTION IF EXISTS wasm_raw_f64;
DROP FUNCTION IF EXISTS wasm_rb_f32;
DROP FUNCTION IF EXISTS wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_float';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_float', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_raw_f32
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'identity_int_float' :: 'identity_raw_f32'
    ARGUMENTS (x Float32) RETURNS Float32;

CREATE FUNCTION wasm_raw_f64
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'identity_int_float' :: 'identity_raw_f64'
    ARGUMENTS (x Float64) RETURNS Float64;

CREATE FUNCTION wasm_rb_f32
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_float' :: 'identity_rowbinary_f32'
    ARGUMENTS (x Float32) RETURNS Float32
    SETTINGS serialization_format = 'RowBinary';

CREATE FUNCTION wasm_rb_f64
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_float' :: 'identity_rowbinary_f64'
    ARGUMENTS (x Float64) RETURNS Float64
    SETTINGS serialization_format = 'RowBinary';

-- ROW_DIRECT Float32 roundtrip (exact binary fractions avoid precision surprises).
SELECT wasm_raw_f32(toFloat32(1.5));
SELECT wasm_raw_f32(toFloat32(-2.5));
SELECT wasm_raw_f32(toFloat32(0.0));

-- ROW_DIRECT Float64 roundtrip.
SELECT wasm_raw_f64(toFloat64(1.5));
SELECT wasm_raw_f64(toFloat64(-2.5));
SELECT wasm_raw_f64(toFloat64(0.0));

-- BUFFERED_V1 RowBinary Float32 roundtrip.
SELECT wasm_rb_f32(toFloat32(1.5));
SELECT wasm_rb_f32(toFloat32(-2.5));
SELECT wasm_rb_f32(toFloat32(0.0));

-- BUFFERED_V1 RowBinary Float64 roundtrip.
SELECT wasm_rb_f64(toFloat64(1.5));
SELECT wasm_rb_f64(toFloat64(-2.5));
SELECT wasm_rb_f64(toFloat64(0.0));

-- Float32 → Float64 widening: both ROW_DIRECT and BUFFERED_V1.
SELECT wasm_raw_f64(toFloat32(1.5));
SELECT wasm_rb_f64(toFloat32(1.5));

-- Multiple rows.
SELECT wasm_rb_f64(toFloat64(number)) FROM numbers(4);

DROP FUNCTION wasm_raw_f32;
DROP FUNCTION wasm_raw_f64;
DROP FUNCTION wasm_rb_f32;
DROP FUNCTION wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_float';
EOF
