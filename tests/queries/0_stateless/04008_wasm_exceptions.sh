#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test WASM native exceptions proposal handling.
# Verifies that:
#   - Uncaught WASM exceptions propagate as WASM_ERROR.
#   - Exceptions caught internally by the WASM module work correctly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS wasm_throw_exception;
DROP FUNCTION IF EXISTS wasm_catch_exception;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_exceptions';

EOF

cat ${CUR_DIR}/wasm/exceptions.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'wasm_exceptions', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION wasm_throw_exception LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_exceptions' :: 'throw_exception' ARGUMENTS (UInt32) RETURNS UInt32;
CREATE FUNCTION wasm_catch_exception LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_exceptions' :: 'catch_exception' ARGUMENTS (UInt32) RETURNS UInt32;

-- Happy path: no exception thrown
SELECT wasm_throw_exception(0 :: UInt32);

-- Exception caught inside WASM module — returns caught value
SELECT wasm_catch_exception(42 :: UInt32);
SELECT wasm_catch_exception(0  :: UInt32);

-- Uncaught WASM exception propagates as WASM_ERROR
SELECT wasm_throw_exception(1 :: UInt32); -- { serverError WASM_ERROR }

DROP FUNCTION wasm_throw_exception;
DROP FUNCTION wasm_catch_exception;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_exceptions';

EOF
