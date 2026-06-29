#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Regression test for BUFFERED_V1 + RowBinary type coercion.
# Before the fix, passing Int8 to an Int32 parameter caused RowBinary to serialize
# 1 byte per value instead of 4, making the WASM module read garbage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_identity_rowbinary_i32;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_rowbinary';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_rowbinary', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_identity_rowbinary_i32
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'identity_int_rowbinary' :: 'identity_rowbinary_i32'
    ARGUMENTS (n Int32) RETURNS Int32
    SETTINGS serialization_format = 'RowBinary';

-- Coercion from small int types: without the fix these return garbage.
SELECT wasm_identity_rowbinary_i32(toInt8(42));
SELECT wasm_identity_rowbinary_i32(toInt8(-100));
SELECT wasm_identity_rowbinary_i32(toUInt8(200));
SELECT wasm_identity_rowbinary_i32(toInt16(-1000));
SELECT wasm_identity_rowbinary_i32(toUInt16(1000));
-- Same declared type: must still work.
SELECT wasm_identity_rowbinary_i32(toInt32(2147483647));
SELECT wasm_identity_rowbinary_i32(toInt32(-2147483648));
-- Multiple rows at once.
SELECT wasm_identity_rowbinary_i32(toInt8(number)) FROM numbers(5);

DROP FUNCTION wasm_identity_rowbinary_i32;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_rowbinary';
EOF
