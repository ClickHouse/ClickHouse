#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test WASM UDF type coercion across all ABIs and serialization formats.
# CH small integer types (Int8/UInt8/Int16/UInt16) coerce to Int32 (i32 WASM kind);
# Int64/UInt64 coerce to Int64 (i64 WASM kind).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS wasm_identity_raw;
DROP FUNCTION IF EXISTS wasm_add;
DROP FUNCTION IF EXISTS wasm_identity_msgpack_i32;
DROP FUNCTION IF EXISTS wasm_identity_msgpack_i64;
DROP FUNCTION IF EXISTS wasm_identity_tsv_i32;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int';

EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION wasm_identity_raw          LANGUAGE WASM ABI ROW_DIRECT  FROM 'identity_int' :: 'identity_raw'             ARGUMENTS (Int32) RETURNS Int32;
CREATE FUNCTION wasm_add               LANGUAGE WASM ABI ROW_DIRECT  FROM 'identity_int' :: 'add'                  ARGUMENTS (Int32, Int32) RETURNS Int32;
CREATE FUNCTION wasm_identity_msgpack_i32 LANGUAGE WASM ABI BUFFERED_V1 FROM 'identity_int' :: 'identity_msgpack_i32' ARGUMENTS (n Int32) RETURNS Int32;
CREATE FUNCTION wasm_identity_msgpack_i64 LANGUAGE WASM ABI BUFFERED_V1 FROM 'identity_int' :: 'identity_msgpack_i64' ARGUMENTS (n Int64) RETURNS Int64;
CREATE FUNCTION wasm_identity_tsv_i32  LANGUAGE WASM ABI BUFFERED_V1 FROM 'identity_int' :: 'identity_tsv_i32'     ARGUMENTS (n Int32) RETURNS Int32 SETTINGS serialization_format = 'TSV';

-- ROW_DIRECT: boundary values for all small-int types
SELECT wasm_identity_raw(toInt8(-128)),   wasm_identity_raw(toInt8(0)),    wasm_identity_raw(toInt8(127));
SELECT wasm_identity_raw(toUInt8(0)),     wasm_identity_raw(toUInt8(128)), wasm_identity_raw(toUInt8(255));
SELECT wasm_identity_raw(toInt16(-32768)),wasm_identity_raw(toInt16(0)),   wasm_identity_raw(toInt16(32767));
SELECT wasm_identity_raw(toUInt16(0)),    wasm_identity_raw(toUInt16(1000)),wasm_identity_raw(toUInt16(65535));
SELECT wasm_add(10, 20), wasm_add(100, 55);
SELECT toInt8(wasm_add(100, 100));

-- BUFFERED_V1 MsgPack: small-int coercions exercise all MsgPack integer encodings
SELECT wasm_identity_msgpack_i32(toInt8(42));       -- fixint
SELECT wasm_identity_msgpack_i32(toInt8(-100));     -- int8
SELECT wasm_identity_msgpack_i32(toUInt8(200));     -- uint8
SELECT wasm_identity_msgpack_i32(toInt16(-1000));   -- int16
SELECT wasm_identity_msgpack_i32(toUInt16(1000));   -- uint16
SELECT wasm_identity_msgpack_i32(toInt32(100000));  -- uint32 (positive)
SELECT wasm_identity_msgpack_i32(toInt32(-100000)); -- int32  (negative)
SELECT wasm_identity_msgpack_i64(toInt64(9223372036854775807));
SELECT wasm_identity_msgpack_i64(toInt64(-9223372036854775808));
SELECT wasm_identity_msgpack_i64(toUInt64(4294967295));

-- BUFFERED_V1 TSV: same small-int coercions via text serialization
SELECT wasm_identity_tsv_i32(toInt8(42));
SELECT wasm_identity_tsv_i32(toInt8(-100));
SELECT wasm_identity_tsv_i32(toUInt8(200));
SELECT wasm_identity_tsv_i32(toInt16(-1000));
SELECT wasm_identity_tsv_i32(toUInt16(1000));
SELECT wasm_identity_tsv_i32(toInt32(2147483647));
SELECT wasm_identity_tsv_i32(toInt32(-2147483648));

DROP FUNCTION wasm_identity_raw;
DROP FUNCTION wasm_add;
DROP FUNCTION wasm_identity_msgpack_i32;
DROP FUNCTION wasm_identity_msgpack_i64;
DROP FUNCTION wasm_identity_tsv_i32;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int';

EOF
