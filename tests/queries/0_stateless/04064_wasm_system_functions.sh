#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that WASM UDFs appear correctly in system.functions with proper metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS wasm_sf_simple;
DROP FUNCTION IF EXISTS wasm_sf_buffered;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_sf_test';
EOF

cat "${CUR_DIR}/wasm/identity_int.wasm" | ${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'wasm_sf_test', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << 'EOF'
-- RowDirect ABI function with a named argument
CREATE OR REPLACE FUNCTION wasm_sf_simple
    LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_sf_test' :: 'identity_raw'
    ARGUMENTS (x Int32) RETURNS Int32;

-- BufferedV1 ABI function
CREATE OR REPLACE FUNCTION wasm_sf_buffered
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'wasm_sf_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32;

-- Both functions must appear in system.functions with origin WasmUserDefined
SELECT name, is_aggregate, case_insensitive, origin, syntax, arguments, returned_value
FROM system.functions
WHERE name IN ('wasm_sf_simple', 'wasm_sf_buffered')
ORDER BY name;

-- Each function must appear exactly once (no duplicates from the SQL UDF storage)
SELECT name, count() AS cnt
FROM system.functions
WHERE name IN ('wasm_sf_simple', 'wasm_sf_buffered')
GROUP BY name
HAVING cnt > 1;

DROP FUNCTION IF EXISTS wasm_sf_simple;
DROP FUNCTION IF EXISTS wasm_sf_buffered;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_sf_test';
EOF
