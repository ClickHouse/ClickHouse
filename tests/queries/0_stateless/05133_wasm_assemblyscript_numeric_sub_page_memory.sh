#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `requiresGuestLinearMemory` answers whether the input crosses through linear memory, not whether
# the module owns any. A numeric-only `ASSEMBLYSCRIPT` signature passes its arguments as
# WebAssembly values, so it is exempt from the sub-page preflight and from input splitting - but
# the module still declares one initial page, so a cap below a page is refused by the
# instantiation itself, naming the requirement the module actually has.

MODULE="as_numeric_${CLICKHOUSE_DATABASE}"
FUNC="wasm_as_numeric_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/as_example.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM '${MODULE}' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32"

echo "a cap below one page is refused by the instantiation, not by a guest allocation"
${CLICKHOUSE_CLIENT} --query "
SELECT ${FUNC}(1::UInt32, 2::UInt32) SETTINGS webassembly_udf_max_memory = 1000" 2>&1 \
  | grep -c -m 1 "memory minimum size of 1 pages exceeds memory limits"

echo "and the same function runs with a cap that holds its page"
${CLICKHOUSE_CLIENT} --query "
SELECT ${FUNC}(1::UInt32, 2::UInt32) SETTINGS webassembly_udf_max_memory = 1048576"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
