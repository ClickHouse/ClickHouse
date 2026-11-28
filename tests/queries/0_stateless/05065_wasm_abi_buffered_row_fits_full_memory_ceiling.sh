#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Budgeting a batch against half the guest's linear memory is a heuristic that leaves the guest room
# for its working set; it is not a statement about what the guest can hold. Splitting therefore
# stops at one row per call and never refuses a row for being past the budget: a row larger than
# half the memory but smaller than all of it still serializes and the guest still allocates it, so
# failing it would turn a batching preference into a hard compatibility limit.

MODULE="full_ceiling_${CLICKHOUSE_DATABASE}"
FUNC="wasm_full_ceiling_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << EOF
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_row_binary'
    ARGUMENTS (x String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'RowBinary';

-- 800000 bytes is above half of the 1572864-byte ceiling and below the ceiling itself.
SELECT 'a row larger than half the linear memory ceiling is passed on its own';
SELECT ${FUNC}(repeat('a', 800000))[1] = 1 AS single_row_batch
SETTINGS max_threads = 1, webassembly_udf_max_input_block_size = 0, webassembly_udf_max_memory = 1572864;

DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
