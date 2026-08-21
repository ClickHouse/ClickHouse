#!/usr/bin/env bash
# A build without a WebAssembly engine must fail close: the server starts even when
# `allow_experimental_webassembly_udf` is enabled in the configuration, `system.webassembly_modules`
# is not exposed at all, and any attempt to define a WebAssembly UDF is rejected with
# `SUPPORT_IS_DISABLED` instead of half-working. On a build with `wasmtime` nothing changes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reaching this point already proves the server started with the configuration used by the test run.
use_wasmtime=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.build_options WHERE name = 'USE_WASMTIME'")
udf_enabled=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'allow_experimental_webassembly_udf'")

expected_available=0
if [[ "$use_wasmtime" == "1" && "$udf_enabled" == "1" ]]; then
    expected_available=1
fi

has_system_table=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'webassembly_modules'")
if [[ "$has_system_table" == "$expected_available" ]]; then
    echo "system.webassembly_modules: OK"
else
    echo "system.webassembly_modules: FAIL, expected $expected_available, got $has_system_table"
fi

# The module does not exist, so the query must fail in any case, but on a build without a WebAssembly
# engine it has to be rejected as unsupported rather than as a missing module.
error=$($CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${CLICKHOUSE_DATABASE}_wasm_udf LANGUAGE WASM ABI ROW_DIRECT FROM 'no_such_module' ARGUMENTS (num UInt32) RETURNS UInt32" 2>&1)
if [[ "$expected_available" == "1" ]]; then
    echo "$error" | grep -q "RESOURCE_NOT_FOUND" && echo "CREATE FUNCTION: OK" || echo "CREATE FUNCTION: FAIL, $error"
else
    echo "$error" | grep -q "SUPPORT_IS_DISABLED" && echo "CREATE FUNCTION: OK" || echo "CREATE FUNCTION: FAIL, $error"
fi
