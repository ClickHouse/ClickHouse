#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# WebAssembly modules and functions are server-wide, so their names carry the database to keep
# concurrent runs of this test apart. Since the export name defaults to the function name, it is given
# explicitly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_host_api_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_func_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_random_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_log_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS export_faulty_malloc_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS export_incorrect_malloc_${CLICKHOUSE_DATABASE};

DELETE FROM system.webassembly_modules WHERE name = 'test_host_api_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_non_env_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'export_faulty_malloc_${CLICKHOUSE_DATABASE}';

EOF

cat ${CUR_DIR}/wasm/host_api.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'test_host_api_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_random_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'test_host_api_${CLICKHOUSE_DATABASE}' :: 'test_random' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT test_random_${CLICKHOUSE_DATABASE}(1 :: UInt32) != test_random_${CLICKHOUSE_DATABASE}(2 :: UInt32);

CREATE FUNCTION test_host_api_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'test_host_api_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;
CREATE FUNCTION test_log_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'test_host_api_${CLICKHOUSE_DATABASE}' :: 'test_log2' ARGUMENTS (UInt32) RETURNS UInt32;

EOF

${CLICKHOUSE_CLIENT}  --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=debug --query \
    "SELECT test_host_api_${CLICKHOUSE_DATABASE}(materialize(0) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_ok' FORMAT Null" 2>&1 | grep -o "Hello, ClickHouse"

${CLICKHOUSE_CLIENT}  --allow_experimental_analyzer=1 --query \
    "SELECT test_host_api_${CLICKHOUSE_DATABASE}(materialize(1) :: UInt32) SETTINGS log_comment = '03208_wasm_import_export_err' FORMAT Null" 2>&1 | grep 'DB::Exception' | grep -o "WebAssembly UDF terminated with error: Goodbye, ClickHouse" | head -1

# clickhouse_log: DEBUG level (7) — appears at debug filter
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=debug --query \
    "SELECT test_log_${CLICKHOUSE_DATABASE}(7 :: UInt32) FORMAT Null" 2>&1 | grep -o "log2_msg"

# clickhouse_log: WARNING level (4) — appears at warning filter
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=warning --query \
    "SELECT test_log_${CLICKHOUSE_DATABASE}(4 :: UInt32) FORMAT Null" 2>&1 | grep -o "log2_msg"

# clickhouse_log: DEBUG level (7) — suppressed at warning filter (level respected)
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=warning --query \
    "SELECT test_log_${CLICKHOUSE_DATABASE}(7 :: UInt32) FORMAT Null" 2>&1 | grep -o "log2_msg" || echo "not_logged"

# clickhouse_log: ERROR level (3) clamped to WARNING — appears at warning filter
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --allow_repeated_settings --send_logs_level=warning --query \
    "SELECT test_log_${CLICKHOUSE_DATABASE}(3 :: UInt32) FORMAT Null" 2>&1 | grep -o "log2_msg"

# clickhouse_log: out-of-range levels clamped to TRACE — no error, function returns 0
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --query "SELECT test_log_${CLICKHOUSE_DATABASE}(100 :: UInt32)"

cat ${CUR_DIR}/wasm/import_unknown.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'import_unknown_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'import_unknown_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError RESOURCE_NOT_FOUND }
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown_${CLICKHOUSE_DATABASE}';

EOF

# Regression: non-"env" imports are skipped by linkHostFunctions (no RESOURCE_NOT_FOUND).
# The module compiles fine; instantiation fails at call time because the runtime
# (Wasmtime) cannot satisfy the WASI import either — WASM_ERROR, not RESOURCE_NOT_FOUND.
cat ${CUR_DIR}/wasm/import_non_env.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'import_non_env_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'import_non_env_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT test_func_${CLICKHOUSE_DATABASE}(1 :: UInt32); -- { serverError WASM_ERROR }
DROP FUNCTION test_func_${CLICKHOUSE_DATABASE};
DELETE FROM system.webassembly_modules WHERE name = 'import_non_env_${CLICKHOUSE_DATABASE}';

EOF

cat ${CUR_DIR}/wasm/import_incorrect.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'import_incorrect_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION test_func_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI ROW_DIRECT FROM 'import_incorrect_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;  -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect_${CLICKHOUSE_DATABASE}';

EOF

cat ${CUR_DIR}/wasm/export_incorrect_malloc.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'export_incorrect_malloc_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_incorrect_malloc_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI BUFFERED_V1 FROM 'export_incorrect_malloc_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32; -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc_${CLICKHOUSE_DATABASE}';

EOF

cat ${CUR_DIR}/wasm/export_faulty_malloc.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'export_faulty_malloc_${CLICKHOUSE_DATABASE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION export_faulty_malloc_${CLICKHOUSE_DATABASE} LANGUAGE WASM ABI BUFFERED_V1 FROM 'export_faulty_malloc_${CLICKHOUSE_DATABASE}' :: 'test_func' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT export_faulty_malloc_${CLICKHOUSE_DATABASE}(1 :: UInt32); -- { serverError WASM_ERROR }

EOF

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS test_host_api_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_func_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_random_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS test_log_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS export_faulty_malloc_${CLICKHOUSE_DATABASE};
DROP FUNCTION IF EXISTS export_incorrect_malloc_${CLICKHOUSE_DATABASE};

DELETE FROM system.webassembly_modules WHERE name = 'test_host_api_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_unknown_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_non_env_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'import_incorrect_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'export_incorrect_malloc_${CLICKHOUSE_DATABASE}';
DELETE FROM system.webassembly_modules WHERE name = 'export_faulty_malloc_${CLICKHOUSE_DATABASE}';

EOF
