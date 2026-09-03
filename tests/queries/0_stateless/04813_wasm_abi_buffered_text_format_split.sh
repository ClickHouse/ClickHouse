#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the BUFFERED_V1 dynamic splitter's size estimate with text
# serialization formats. The splitter prices values by their in-memory column width;
# text renderings are larger (an Int8 of -100 is 1 byte in memory but 5 bytes as CSV),
# so without a worst-case wire-expansion factor the splitter skips a needed split, the
# whole block is serialized at ~5x the estimated size, and the guest's allocator
# (deliberately given a small 1 MiB heap in text_split_abi.c) rejects the buffer,
# failing the query with `WASM_ERROR` even though splitting would have made it succeed.
#
# The guest returns, for every input row, the number of rows in the batch it arrived
# in, so SQL can assert both that every row was processed and that the 256 KiB block
# was actually split into smaller batches.

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS wasm_csv_batch_rows;
DELETE FROM system.webassembly_modules WHERE name = 'text_split_abi';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'text_split_abi', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION wasm_csv_batch_rows
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'text_split_abi' :: 'batch_row_count'
    ARGUMENTS (x Int8) RETURNS UInt32
    SETTINGS serialization_format = 'CSV';

-- A single 262144-row block of Int8 values -100/-99: ~256 KiB in memory (within the
-- ~550 KiB input budget derived from the module's linear memory, so the unscaled
-- estimate would not split), but ~1.2 MiB as CSV text — more than the guest's 1 MiB
-- heap can hold in one piece.
SELECT count() = 262144 AS all_rows_processed,
       max(batch_rows) < 262144 AS was_split,
       min(batch_rows) >= 1 AS batches_non_empty
FROM
(
    SELECT wasm_csv_batch_rows(toInt8((number % 2) - 100)) AS batch_rows
    FROM numbers(262144)
)
SETTINGS max_block_size = 262144, max_threads = 1, webassembly_udf_max_input_block_size = 0;

DROP FUNCTION IF EXISTS wasm_csv_batch_rows;
DELETE FROM system.webassembly_modules WHERE name = 'text_split_abi';
EOF
