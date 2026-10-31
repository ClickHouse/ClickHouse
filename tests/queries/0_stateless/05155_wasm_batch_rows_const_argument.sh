#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `ColumnConst` argument is one stored row broadcast to the batch, and a wire that carries
# constness writes that row once, so the argument costs the same bytes at one row as at a whole
# block. When it alone exceeds the input budget, no row count brings a call inside the budget,
# and every call re-pays those bytes. The splitter must then budget the rows on top of that fixed
# part: neither collapse to one row per call, nor hand over the whole block, which would leave the
# call unbounded.
#
# The module declares exactly 1 MiB of linear memory, so with a ratio of 0.002 the budget is
# 2097 bytes and the 4000-byte constant below is over it whatever the compiler does.

MODULE="cb_batch_const_${CLICKHOUSE_DATABASE}"
FUNC="wasm_cb_batch_const_${CLICKHOUSE_DATABASE}"
FUNC_TYPED="wasm_cb_batch_const_typed_${CLICKHOUSE_DATABASE}"
FUNC_WIDE="wasm_cb_batch_const_wide_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DROP FUNCTION IF EXISTS ${FUNC_TYPED};
DROP FUNCTION IF EXISTS ${FUNC_WIDE};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"

cat "${CUR_DIR}/wasm/columnar_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI COLUMNAR_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (s String, wide String) RETURNS UInt64;"

${CLICKHOUSE_CLIENT} --query "
SELECT 'oversized const does not force a batch of one row: ' || toString(max(batch_rows) > 10),
       'oversized const does not take the whole block: ' || toString(max(batch_rows) < 1000),
       'all rows processed: ' || toString(count() = 1000)
FROM
(
    SELECT ${FUNC}(substring(toString(number), 1, 1), repeat('a', 4000)) AS batch_rows
    FROM numbers(1000)
)
SETTINGS max_block_size = 1000, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

# The constant is priced against its *declared* type, which is found by its position in the
# function's argument list. A declared list whose types differ, with the constant anywhere but
# first, is what tells a wrong lookup apart.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_TYPED}
    LANGUAGE WASM ABI COLUMNAR_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (n UInt64, wide String) RETURNS UInt64;"

${CLICKHOUSE_CLIENT} --query "
SELECT 'const argument keeps its declared type: ' || toString(max(batch_rows) > 10),
       'all rows processed: ' || toString(count() = 1000)
FROM
(
    SELECT ${FUNC_TYPED}(number, repeat('a', 4000)) AS batch_rows
    FROM numbers(1000)
)
SETTINGS max_block_size = 1000, max_threads = 1, webassembly_udf_max_input_block_size = 0,
         webassembly_udf_input_split_memory_ratio = 0.002;"

# The frame header and one descriptor per argument are paid once per call even with no constant
# argument at all. Sixty narrow arguments cost 16 + 60 * 40 = 2416 bytes before the first row,
# over the 2097-byte budget, so no row count fits; the rows are still budgeted on top of it.
${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC_WIDE}
    LANGUAGE WASM ABI COLUMNAR_V1
    FROM '${MODULE}' :: 'batch_row_count_col'
    ARGUMENTS (a0 UInt8, a1 UInt8, a2 UInt8, a3 UInt8, a4 UInt8, a5 UInt8, a6 UInt8, a7 UInt8, a8 UInt8, a9 UInt8, a10 UInt8, a11 UInt8, a12 UInt8, a13 UInt8, a14 UInt8, a15 UInt8, a16 UInt8, a17 UInt8, a18 UInt8, a19 UInt8, a20 UInt8, a21 UInt8, a22 UInt8, a23 UInt8, a24 UInt8, a25 UInt8, a26 UInt8, a27 UInt8, a28 UInt8, a29 UInt8, a30 UInt8, a31 UInt8, a32 UInt8, a33 UInt8, a34 UInt8, a35 UInt8, a36 UInt8, a37 UInt8, a38 UInt8, a39 UInt8, a40 UInt8, a41 UInt8, a42 UInt8, a43 UInt8, a44 UInt8, a45 UInt8, a46 UInt8, a47 UInt8, a48 UInt8, a49 UInt8, a50 UInt8, a51 UInt8, a52 UInt8, a53 UInt8, a54 UInt8, a55 UInt8, a56 UInt8, a57 UInt8, a58 UInt8, a59 UInt8) RETURNS UInt64;"

${CLICKHOUSE_CLIENT} --query "
SELECT 'oversized framing does not force a batch of one row: ' || toString(max(batch_rows) > 10),
       'oversized framing does not take the whole block: ' || toString(max(batch_rows) < 100),
       'all rows processed: ' || toString(count() = 100)
FROM
(
    SELECT ${FUNC_WIDE}(toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number), toUInt8(number)) AS batch_rows
    FROM numbers(100)
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
DROP FUNCTION IF EXISTS ${FUNC_WIDE};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"
