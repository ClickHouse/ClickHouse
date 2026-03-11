#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_host_api;
DROP FUNCTION IF EXISTS test_func;
DROP FUNCTION IF EXISTS test_random;
DROP FUNCTION IF EXISTS export_faulty_malloc;
DROP FUNCTION IF EXISTS export_incorrect_malloc;

DELETE FROM system.webassembly_modules WHERE name = 'test_host_api';
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown';
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect';
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc';
DELETE FROM system.webassembly_modules WHERE name = 'export_faulty_malloc';

EOF

cat ${CUR_DIR}/wasm/host_api.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'test_host_api', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_random LANGUAGE WASM ABI ROW_DIRECT FROM 'test_host_api' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT test_random(1 :: UInt32) != test_random(2 :: UInt32);

CREATE FUNCTION test_host_api LANGUAGE WASM ABI ROW_DIRECT FROM 'test_host_api' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;

EOF

${CLICKHOUSE_CLIENT}  --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=trace --query \
    "SELECT test_host_api(materialize(0) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_ok' FORMAT Null" 2>&1 | grep -o "Hello, ClickHouse"

${CLICKHOUSE_CLIENT}  --allow_experimental_analyzer=1 --query \
    "SELECT test_host_api(materialize(1) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_err' FORMAT Null" 2>&1 | grep 'DB::Exception' | grep -o "WebAssembly UDF terminated with error: Goodbye, ClickHouse" | head -1

cat ${CUR_DIR}/wasm/import_unknown.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'import_unknown', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func LANGUAGE WASM ABI ROW_DIRECT FROM 'import_unknown' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError RESOURCE_NOT_FOUND }
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown';

EOF

cat ${CUR_DIR}/wasm/import_incorrect.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'import_incorrect', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func LANGUAGE WASM ABI ROW_DIRECT FROM 'import_incorrect' ARGUMENTS (UInt32) RETURNS UInt32;  -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect';

EOF

cat ${CUR_DIR}/wasm/export_incorrect_malloc.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'export_incorrect_malloc', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_incorrect_malloc LANGUAGE WASM ABI BUFFERED_V1 FROM 'export_incorrect_malloc' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc';

EOF

cat ${CUR_DIR}/wasm/export_faulty_malloc.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'export_faulty_malloc', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_faulty_malloc LANGUAGE WASM ABI BUFFERED_V1 FROM 'export_faulty_malloc' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT export_faulty_malloc(1 :: UInt32); -- { serverError WASM_ERROR }

EOF

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_host_api;
DROP FUNCTION IF EXISTS test_func;
DROP FUNCTION IF EXISTS test_random;
DROP FUNCTION IF EXISTS export_faulty_malloc;
DROP FUNCTION IF EXISTS export_incorrect_malloc;

DELETE FROM system.webassembly_modules WHERE name = 'test_host_api';
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown';
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect';
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc';
DELETE FROM system.webassembly_modules WHERE name = 'export_faulty_malloc';

EOF
