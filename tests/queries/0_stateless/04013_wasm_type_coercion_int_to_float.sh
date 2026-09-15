#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Integer-to-float widening coercions in ROW_DIRECT and BUFFERED_V1 RowBinary.
# Any integer type (i32 or i64 WASM kind) can be passed to a Float32/Float64 parameter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS test_04013_wasm_raw_f32;
DROP FUNCTION IF EXISTS test_04013_wasm_raw_f64;
DROP FUNCTION IF EXISTS test_04013_wasm_rb_f32;
DROP FUNCTION IF EXISTS test_04013_wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_i2f';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_i2f', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION test_04013_wasm_raw_f32
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'identity_int_i2f' :: 'identity_raw_f32'
    ARGUMENTS (x Float32) RETURNS Float32;

CREATE FUNCTION test_04013_wasm_raw_f64
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'identity_int_i2f' :: 'identity_raw_f64'
    ARGUMENTS (x Float64) RETURNS Float64;

CREATE FUNCTION test_04013_wasm_rb_f32
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_i2f' :: 'identity_rowbinary_f32'
    ARGUMENTS (x Float32) RETURNS Float32
    SETTINGS serialization_format = 'RowBinary';

CREATE FUNCTION test_04013_wasm_rb_f64
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_i2f' :: 'identity_rowbinary_f64'
    ARGUMENTS (x Float64) RETURNS Float64
    SETTINGS serialization_format = 'RowBinary';

-- ROW_DIRECT: all integer types → Float32.
SELECT test_04013_wasm_raw_f32(toInt8(4));
SELECT test_04013_wasm_raw_f32(toInt16(8));
SELECT test_04013_wasm_raw_f32(toInt32(16));
SELECT test_04013_wasm_raw_f32(toInt64(1024));
SELECT test_04013_wasm_raw_f32(toUInt8(32));
SELECT test_04013_wasm_raw_f32(toUInt16(64));
SELECT test_04013_wasm_raw_f32(toUInt32(128));
SELECT test_04013_wasm_raw_f32(toUInt64(2048));

-- ROW_DIRECT: all integer types → Float64.
SELECT test_04013_wasm_raw_f64(toInt8(4));
SELECT test_04013_wasm_raw_f64(toInt16(8));
SELECT test_04013_wasm_raw_f64(toInt32(16));
SELECT test_04013_wasm_raw_f64(toInt64(1024));
SELECT test_04013_wasm_raw_f64(toUInt8(32));
SELECT test_04013_wasm_raw_f64(toUInt16(64));
SELECT test_04013_wasm_raw_f64(toUInt32(128));
SELECT test_04013_wasm_raw_f64(toUInt64(2048));

-- BUFFERED_V1 RowBinary: all integer types → Float32.
SELECT test_04013_wasm_rb_f32(toInt8(4));
SELECT test_04013_wasm_rb_f32(toInt16(8));
SELECT test_04013_wasm_rb_f32(toInt32(16));
SELECT test_04013_wasm_rb_f32(toInt64(1024));
SELECT test_04013_wasm_rb_f32(toUInt8(32));
SELECT test_04013_wasm_rb_f32(toUInt16(64));
SELECT test_04013_wasm_rb_f32(toUInt32(128));
SELECT test_04013_wasm_rb_f32(toUInt64(2048));

-- BUFFERED_V1 RowBinary: all integer types → Float64.
SELECT test_04013_wasm_rb_f64(toInt8(4));
SELECT test_04013_wasm_rb_f64(toInt16(8));
SELECT test_04013_wasm_rb_f64(toInt32(16));
SELECT test_04013_wasm_rb_f64(toInt64(1024));
SELECT test_04013_wasm_rb_f64(toUInt8(32));
SELECT test_04013_wasm_rb_f64(toUInt16(64));
SELECT test_04013_wasm_rb_f64(toUInt32(128));
SELECT test_04013_wasm_rb_f64(toUInt64(2048));

-- Multiple rows: Int8 column → Float64 function.
SELECT test_04013_wasm_rb_f64(toInt8(number)) FROM numbers(4);

DROP FUNCTION test_04013_wasm_raw_f32;
DROP FUNCTION test_04013_wasm_raw_f64;
DROP FUNCTION test_04013_wasm_rb_f32;
DROP FUNCTION test_04013_wasm_rb_f64;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_i2f';
EOF
