#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A module whose exported `memory` starts at zero pages but declares a positive maximum can still
# grow, so its reachable ceiling is that maximum rather than nothing. The splitter derives the
# input budget from the ceiling in that case, so such a module must still batch a block that does
# not fit instead of handing the whole block to the guest and failing late in its allocation.
#
# `growable_zero_page_abi.wasm` is linked as `memory 0 3` and grows its own memory on demand.

MODULE="growable_zero_page_${CLICKHOUSE_DATABASE}"
FUNC="wasm_growable_zero_page_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << QUERIES
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
QUERIES

cat "${CUR_DIR}/wasm/growable_zero_page_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${FUNC}
        LANGUAGE WASM ABI BUFFERED_V1
        FROM '${MODULE}' :: 'batch_row_count'
        ARGUMENTS (x String) RETURNS Array(UInt32)
        SETTINGS serialization_format = 'CSV'"

echo 'A module whose memory starts at zero pages but can grow still splits the block'
${CLICKHOUSE_CLIENT} --query \
    "SELECT max(batch_rows) BETWEEN 1 AND 63 AS split_into_batches
     FROM
     (
         SELECT ${FUNC}(repeat('x', 10000))[1] AS batch_rows FROM numbers(64)
     )
     SETTINGS webassembly_udf_max_input_block_size = 0, max_block_size = 64, max_threads = 1"

${CLICKHOUSE_CLIENT} << QUERIES
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
QUERIES
