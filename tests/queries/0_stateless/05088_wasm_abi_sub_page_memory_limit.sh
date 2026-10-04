#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Linear memory grows in whole 64 KiB pages and the limiter refuses a growth that would cross the
# cap, so a `webassembly_udf_max_memory` below one page lets the guest hold no memory at all.
# There is no honest ceiling to report for such a compartment - zero means "nothing bounds growth"
# and the raw cap names a size the guest can never reach - so the configuration is rejected.

MODULE="sub_page_limit_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

cat "${CUR_DIR}/wasm/small_memory_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${MODULE}_f
        LANGUAGE WASM ABI BUFFERED_V1
        FROM '${MODULE}' :: 'input_size_json'
        ARGUMENTS (x String) RETURNS Array(UInt32)
        SETTINGS serialization_format = 'JSONEachRow'"

echo 'A memory limit below one page is rejected'
${CLICKHOUSE_CLIENT} --query \
    "SELECT ${MODULE}_f('abc') SETTINGS webassembly_udf_max_memory = 1000" 2>&1 \
  | grep -o 'WebAssembly memory limit is 1000 bytes, which is less than a single 65536 byte page' | head -n 1

# A one-page limit passes this validation: what stops the call is the module's own minimum of two
# pages, reported by the engine at instantiation, not the sub-page check above.
echo 'A one-page limit passes the check and reaches instantiation'
${CLICKHOUSE_CLIENT} --query \
    "SELECT ${MODULE}_f('abc') SETTINGS webassembly_udf_max_memory = 65536" 2>&1 \
  | grep -o 'memory minimum size of 2 pages exceeds memory limits' | head -n 1

echo 'A limit that fits the module runs the function'
${CLICKHOUSE_CLIENT} --query \
    "SELECT ${MODULE}_f('abc') SETTINGS webassembly_udf_max_memory = 131072"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${MODULE}_f"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
