#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# The splitter sizes a call by measuring the batch it is about to send, not by summing rows
# measured on their own. A row has no cost of its own under a columnar wire: a `String` column
# writes one `uint64` offset per row plus the characters, so a batch of `n` one-byte strings
# costs `8 * (n + 1) + n` bytes, while measuring each row alone charges `8 * 2 + 1` to every one
# of them - an extra offset per row on top of the frame header and the descriptor table, which
# no fixed per-write subtraction can remove.
#
# The guest returns the number of rows it was handed, so the batch sizes the splitter chose can
# be asserted from SQL. The module declares exactly 1 MiB of linear memory, which is what the
# budget is derived from, so the numbers below do not depend on the compiler.

MODULE="cb_batch_measure_${CLICKHOUSE_DATABASE}"
FUNC="wasm_cb_batch_measure_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"

cat "${CUR_DIR}/wasm/columnar_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (s String) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';"

# The budget is 1 MiB * 0.002 = 2097 bytes, of which the frame header and the single descriptor
# take 56. Measuring the batch, a one-byte row costs 9 bytes and about 226 rows fit; summing
# rows measured alone, each costs 17 and no more than 120 can ever fit. 128 lies between the
# two, so the assertion holds whichever way the arithmetic rounds, but only for a batch that is
# measured whole.
${CLICKHOUSE_CLIENT} --query "
SELECT 'batch measured whole: ' || toString(max(batch_rows) > 128),
       'all rows processed: ' || toString(count() = 65536),
       'batches non-empty: ' || toString(min(batch_rows) >= 1)
FROM
(
    SELECT ${FUNC}(substring(toString(number), 1, 1)) AS batch_rows
    FROM numbers(65536)
)
SETTINGS max_block_size = 65536, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"
