#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A module declaring `memory 0 0` bounds its own linear memory at zero bytes, so no input can ever
# be handed to it. That is a ceiling of zero, not the absence of a ceiling: the call is reported
# against the module's own maximum instead of failing later inside the guest allocator.
#
# No toolchain emits this shape, so the module is assembled here: three types, three functions,
# a memory with minimum and maximum both zero, exports for the memory and for the three functions
# the buffered ABI requires, and bodies returning zero.

MODULE="zero_page_buffered_${CLICKHOUSE_DATABASE}"
WASM="${CLICKHOUSE_TMP}/zero_page_buffered_${CLICKHOUSE_DATABASE}.wasm"

python3 -c "
import sys
sys.stdout.buffer.write(bytes.fromhex(
    '0061736d01000000'                                    # magic and version
    '0110' '03' '60027f7f017f' '60017f017f' '60017f00'    # types: (i32,i32)->i32, (i32)->i32, (i32)->()
    '0304' '03' '000102'                                  # functions: one of each type
    '0504' '01' '01' '00' '00'                            # memory: minimum 0, maximum 0
    '074e' '04'                                           # exports
        '06' '6d656d6f7279' '0200'                        #   memory
        '0a' '62756666657265645f66' '0000'                #   buffered_f
        '18' '636c69636b686f7573655f6372656174655f627566666572' '0001'   # clickhouse_create_buffer
        '19' '636c69636b686f7573655f64657374726f795f627566666572' '0002' # clickhouse_destroy_buffer
    '0a0e' '03' '040041000b' '040041000b' '02000b'))      # code: return 0, return 0, return
" > "${WASM}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

cat "${WASM}" | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

echo 'A BUFFERED_V1 function of a module that cannot hold any memory is created'
${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${MODULE}_f
        LANGUAGE WASM ABI BUFFERED_V1
        FROM '${MODULE}' :: 'buffered_f'
        ARGUMENTS (a UInt32) RETURNS UInt32" 2>&1 | head -n 1

echo 'and calling it reports the zero maximum linear memory of the module'
${CLICKHOUSE_CLIENT} --query "SELECT ${MODULE}_f(1)" 2>&1 \
    | grep -m 1 -o "maximum linear memory of the module is 0 bytes" | wc -l | tr -d ' '

echo 'A function without arguments is reported the same way, having no row to blame'
${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${MODULE}_nullary
        LANGUAGE WASM ABI BUFFERED_V1
        FROM '${MODULE}' :: 'buffered_f'
        ARGUMENTS () RETURNS UInt32" 2>&1 | head -n 1
${CLICKHOUSE_CLIENT} --query "SELECT ${MODULE}_nullary()" 2>&1 \
    | grep -m 1 -o "maximum linear memory of the module is 0 bytes" | wc -l | tr -d ' '

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${MODULE}_nullary"
${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${MODULE}_f"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
rm -f "${WASM}"
