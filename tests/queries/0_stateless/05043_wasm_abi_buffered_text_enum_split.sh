#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the `BUFFERED_V1` dynamic splitter's size estimate when a text
# `serialization_format` carries an `Enum` that is wrapped or nested rather than bare.
#
# The splitter prices values by their in-memory column width, and an `Enum8` is one byte
# wide, but the text formats serialize the enum's *label*. A bare top-level `Enum` can be
# special-cased on the declared argument type, but wrapped shapes cannot be seen that way:
# both `Array(Enum8)` and `Tuple(Enum8, Enum8)` fall through to the fixed-width branch of
# the per-row complex estimator, which prices them by the nested storage width only.
# With only the generic text expansion factor those shapes are under-budgeted, the splitter
# skips a split it needed, and the guest's allocator (deliberately given a small 1 MiB heap
# in `text_split_abi.c`) then rejects the oversized buffer and the query fails with
# `WASM_ERROR` even though smaller batches would have succeeded.
#
# The guest returns, for every input row, the number of rows in the batch it arrived in, so
# SQL can assert both that every row was processed and that the block really was split.

LABEL="aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff"
ENUM="Enum8('${LABEL}' = 1)"
MODULE="text_enum_split_${CLICKHOUSE_DATABASE}"
FUNC_TUP="wasm_tuple_enum_batch_rows_${CLICKHOUSE_DATABASE}"
FUNC_ARR="wasm_arr_enum_batch_rows_${CLICKHOUSE_DATABASE}"
ARR_ELEMS="'${LABEL}', '${LABEL}', '${LABEL}', '${LABEL}', '${LABEL}', '${LABEL}', '${LABEL}', '${LABEL}'"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC_TUP};
DROP FUNCTION IF EXISTS ${FUNC_ARR};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
-- A 60-character label: 60x wider on the CSV wire than the \`Enum8\`'s one-byte storage.
CREATE OR REPLACE FUNCTION ${FUNC_TUP}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (x Tuple(${ENUM}, ${ENUM})) RETURNS UInt32
    SETTINGS serialization_format = 'CSV';

CREATE OR REPLACE FUNCTION ${FUNC_ARR}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (x Array(${ENUM})) RETURNS UInt32
    SETTINGS serialization_format = 'CSV';

SELECT 'Tuple(Enum8, Enum8)';
SELECT count() = 32768 AS all_rows_processed,
       max(batch_rows) < 32768 AS was_split,
       min(batch_rows) >= 1 AS batches_non_empty
FROM
(
    SELECT ${FUNC_TUP}(CAST((('${LABEL}', '${LABEL}')) AS Tuple(${ENUM}, ${ENUM}))) AS batch_rows
    FROM numbers(32768)
)
SETTINGS max_block_size = 32768, max_threads = 1, webassembly_udf_max_input_block_size = 0;

SELECT 'Array(Enum8)';
SELECT count() = 4096 AS all_rows_processed,
       max(batch_rows) < 4096 AS was_split,
       min(batch_rows) >= 1 AS batches_non_empty
FROM
(
    SELECT ${FUNC_ARR}(CAST([${ARR_ELEMS}] AS Array(${ENUM}))) AS batch_rows
    FROM numbers(4096)
)
SETTINGS max_block_size = 4096, max_threads = 1, webassembly_udf_max_input_block_size = 0;

DROP FUNCTION IF EXISTS ${FUNC_TUP};
DROP FUNCTION IF EXISTS ${FUNC_ARR};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
