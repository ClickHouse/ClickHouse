#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `webassembly_udf_max_memory` below one page leaves the guest unable to hold any linear memory,
# which makes a call impossible for an ABI that has to hand the guest a buffer. The `ROW_DIRECT`
# ABI passes its arguments as WebAssembly values and never reads or writes guest memory, so the
# setting says nothing about whether such a call can run and must not reject it.
#
# The module is assembled here, as in `05100`: `memory 0 0` proves the point without relying on a
# toolchain-produced module that happens to declare pages it never needs.

MODULE="row_direct_sub_page_${CLICKHOUSE_DATABASE}"
WASM="${CLICKHOUSE_TMP}/row_direct_sub_page_${CLICKHOUSE_DATABASE}.wasm"

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

${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${MODULE}_f
        LANGUAGE WASM ABI ROW_DIRECT
        FROM '${MODULE}' :: 'zero_pages'
        ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32"

echo 'A sub-page memory limit does not stop a ROW_DIRECT call'
${CLICKHOUSE_CLIENT} --query \
    "SELECT ${MODULE}_f(1, 2) SETTINGS webassembly_udf_max_memory = 1000"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${MODULE}_f"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
rm -f "${WASM}"
