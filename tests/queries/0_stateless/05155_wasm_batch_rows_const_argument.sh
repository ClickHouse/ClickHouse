#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# A `ColumnConst` argument is one stored row broadcast to the batch, and a wire that carries
# constness writes that row once, so the argument costs the same bytes at one row as at a whole
# block. When it alone exceeds the input budget, no row count brings a call inside the budget:
# splitting only re-pays those same bytes once per call, and takes from the guest whatever it
# amortizes across a call. The splitter must hand over the whole block instead of collapsing to
# one row per call.
#
# The module declares exactly 1 MiB of linear memory, so with a ratio of 0.002 the budget is
# 2097 bytes and the 4000-byte constant below is over it whatever the compiler does.

MODULE="cb_batch_const_${CLICKHOUSE_DATABASE}"
FUNC="wasm_cb_batch_const_${CLICKHOUSE_DATABASE}"
FUNC_TYPED="wasm_cb_batch_const_typed_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DROP FUNCTION IF EXISTS ${FUNC_TYPED};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"

cat "${CUR_DIR}/wasm/columnar_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (s String, wide String) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';"

${CLICKHOUSE_CLIENT} --query "
SELECT 'oversized const does not force a batch of one row: ' || toString(min(batch_rows) = 1000),
       'all rows processed: ' || toString(count() = 1000)
FROM
(
    SELECT ${FUNC}(substring(toString(number), 1, 1), repeat('a', 4000)) AS batch_rows
    FROM numbers(1000)
)
SETTINGS max_block_size = 1000, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

# The constant is measured on its own to price the part of the batch no row count changes, and
# what it is measured against is its *declared* type, which is found by its position in the
# function's argument list rather than by where it lands in that subset. A declared list whose
# types differ, with the constant anywhere but first, is what tells the two apart.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_TYPED}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (n UInt64, wide String) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';"

${CLICKHOUSE_CLIENT} --query "
SELECT 'const argument keeps its declared type: ' || toString(min(batch_rows) = 1000),
       'all rows processed: ' || toString(count() = 1000)
FROM
(
    SELECT ${FUNC_TYPED}(number, repeat('a', 4000)) AS batch_rows
    FROM numbers(1000)
)
SETTINGS max_block_size = 1000, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

# The same argument materialized is a wide row like any other, and a wide row does shrink out of
# a batch, so there the splitter is right to cut the call down. This is what tells the assertions
# above apart from ones that would pass because nothing splits at all.
${CLICKHOUSE_CLIENT} --query "
SELECT 'materialized wide argument still splits: ' || toString(max(batch_rows) < 10),
       'all rows processed: ' || toString(count() = 1000)
FROM
(
    SELECT ${FUNC}(substring(toString(number), 1, 1), materialize(repeat('a', 4000))) AS batch_rows
    FROM numbers(1000)
)
SETTINGS max_block_size = 1000, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DROP FUNCTION IF EXISTS ${FUNC_TYPED};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"
