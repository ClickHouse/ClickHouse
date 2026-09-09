#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# A `ColumnBinary` frame carries a fixed header and one fixed-size descriptor per column
# whatever the row count, so a call pays that metadata once. The splitter measures a row by
# serializing it alone and subtracting `blockFramingBytes`, so a format whose framing that
# helper does not know is charged its whole frame metadata to every single row. With enough
# columns the metadata dwarfs the rows, and the splitter then cuts a block that comfortably
# fits one call into one call per row - the opposite of what the batching is for, and visible
# to the guest as its row count.
#
# The guest returns the number of rows it was handed, so the batch sizes the splitter chose can
# be asserted from SQL. The module declares exactly 1 MiB of linear memory, which is what the
# budget is derived from, so the numbers below do not depend on the compiler.

MODULE="cb_framing_split_${CLICKHOUSE_DATABASE}"
FUNC="wasm_cb_framing_split_${CLICKHOUSE_DATABASE}"

NUM_COLS=20
NUM_ROWS=64

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/columnar_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

# 20 `UInt64` columns: 16 + 20 * 40 = 816 bytes of frame metadata against 160 bytes of rows.
args=""
call_args=""
for ((i = 0; i < NUM_COLS; ++i)); do
    [ -n "${args}" ] && args="${args}, " && call_args="${call_args}, "
    args="${args}a${i} UInt64"
    call_args="${call_args}number + ${i}"
done

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (${args}) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';"

# The budget is 1 MiB * 0.0015 = 1572 bytes. Charged correctly, the frame metadata is paid once
# and about four rows share a call; charged per row, every row costs 816 + 160 bytes and no two
# rows ever fit together.
${CLICKHOUSE_CLIENT} --query "
SELECT 'frame metadata charged once per call: ' || toString(max(batch_rows) > 1)
FROM
(
    SELECT ${FUNC}(${call_args}) AS batch_rows
    FROM numbers(${NUM_ROWS})
)
SETTINGS max_block_size = ${NUM_ROWS}, max_threads = 1,
    webassembly_udf_max_input_block_size = 0,
    webassembly_udf_input_split_memory_ratio = 0.0015;"

# ... and the block is still split, so the check above is not passing merely because splitting
# stopped happening altogether.
${CLICKHOUSE_CLIENT} --query "
SELECT 'block still split: ' || toString(max(batch_rows) < ${NUM_ROWS})
FROM
(
    SELECT ${FUNC}(${call_args}) AS batch_rows
    FROM numbers(${NUM_ROWS})
)
SETTINGS max_block_size = ${NUM_ROWS}, max_threads = 1,
    webassembly_udf_max_input_block_size = 0,
    webassembly_udf_input_split_memory_ratio = 0.0015;"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
