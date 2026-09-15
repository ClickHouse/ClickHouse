#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Regression: cross-kind numeric coercion must be rejected at type-check time.
# Float64/Float32 passed to an Int32 WASM function must produce an error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_04007_wasm_identity_raw;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_04007';

EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_int_04007', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_04007_wasm_identity_raw LANGUAGE WASM ABI ROW_DIRECT FROM 'identity_int_04007' :: 'identity_raw' ARGUMENTS (Int32) RETURNS Int32;

-- Cross-kind: Float64 → i32 must be rejected.
SELECT test_04007_wasm_identity_raw(toFloat64(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Cross-kind: Float32 → i32 must be rejected.
SELECT test_04007_wasm_identity_raw(toFloat32(1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP FUNCTION test_04007_wasm_identity_raw;
DELETE FROM system.webassembly_modules WHERE name = 'identity_int_04007';

EOF
