#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `getArgumentsBlock` used to materialize a constant argument across the whole block before
# cutting the requested range out of it, so every single-row write of the measurement pass
# replicated the constant `row_count` times: quadratic in the number of rows. Cutting first
# (`ColumnConst::cut` is O(1)) keeps it linear. The `max_execution_time` below is two orders of
# magnitude above what the fixed version needs and far below what the quadratic one does.

MODULE="const_cut_${CLICKHOUSE_DATABASE}"
FUNC="wasm_const_cut_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_json'
    ARGUMENTS (s String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'JSONEachRow'"

echo 'a wide constant argument is cut before it is materialized'
${CLICKHOUSE_CLIENT} --query "
SELECT max(batch_rows) > 0 AS split_into_batches
FROM
(
    SELECT ${FUNC}(repeat('a', 50000))[1] AS batch_rows
    FROM numbers(128)
)
SETTINGS max_block_size = 128, max_threads = 1, webassembly_udf_max_input_block_size = 0, webassembly_udf_max_memory = 1572864, max_execution_time = 60"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
