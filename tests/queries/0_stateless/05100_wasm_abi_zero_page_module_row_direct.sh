#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A module declaring `memory 0 0` can never allocate, so it is unusable for the buffered ABI, which
# has to hand the guest an input buffer. The `ROW_DIRECT` ABI passes its arguments as WebAssembly
# values and never allocates in guest memory, so such a module stays callable.
#
# No toolchain emits this shape, so the module is assembled here: the `wasm` header, an `(i32, i32) -> i32`
# type, one function of that type, a memory with minimum and maximum both zero, exports for the
# memory and the function, and a body returning zero.

MODULE="zero_page_row_direct_${CLICKHOUSE_DATABASE}"
WASM="${CLICKHOUSE_TMP}/zero_page_row_direct_${CLICKHOUSE_DATABASE}.wasm"

python3 -c "
import sys
sys.stdout.buffer.write(bytes.fromhex(
    '0061736d01000000'                                  # magic and version
    '01070160027f7f017f'                                # type section: (i32, i32) -> i32
    '03020100'                                          # function section: one function of type 0
    '050401010000'                                      # memory section: minimum 0, maximum 0
    '0717' '0206' '6d656d6f7279' '0200'                 # export: memory
    '0a' '7a65726f5f7061676573' '0000'                  # export: zero_pages
    '0a0601040041000b'))                                # code section: return 0
" > "${WASM}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

cat "${WASM}" | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

echo 'A ROW_DIRECT function of a module that cannot grow its memory is created'
${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${MODULE}_f
        LANGUAGE WASM ABI ROW_DIRECT
        FROM '${MODULE}' :: 'zero_pages'
        ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32" 2>&1 | head -n 1

echo 'and it is callable'
${CLICKHOUSE_CLIENT} --query "SELECT ${MODULE}_f(1, 2)"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${MODULE}_f"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
rm -f "${WASM}"
