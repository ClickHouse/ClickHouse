#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A zero `webassembly_udf_input_split_memory_ratio` is the explicit opt-out from the dynamic
# splitter: no part of the memory is set aside for a call's input, so
# `webassembly_udf_max_input_block_size = 0` keeps its original meaning - one call per pipeline
# block - and the number of rows a guest observes stays a property of the pipeline rather than of
# the data, which `compatibility` with an older version restores as well.

MODULE="split_opt_out_${CLICKHOUSE_DATABASE}"
FUNC="wasm_split_opt_out_${CLICKHOUSE_DATABASE}"

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
    FROM '${MODULE}' :: 'batch_row_count_json'
    ARGUMENTS (x String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'JSONEachRow';

SELECT 'Dynamic splitting on: the block is split by estimated size';
SELECT max(batch_rows) < 4096 AS split_into_batches
FROM
(
    SELECT ${FUNC}(repeat('a', 200))[1] AS batch_rows
    FROM numbers(4096)
)
SETTINGS max_block_size = 4096, max_threads = 1, webassembly_udf_max_input_block_size = 0;

SELECT 'Dynamic splitting off: one call per pipeline block';
SELECT max(batch_rows) AS batch_rows
FROM
(
    SELECT ${FUNC}(repeat('a', 200))[1] AS batch_rows
    FROM numbers(4096)
)
SETTINGS max_block_size = 4096, max_threads = 1, webassembly_udf_max_input_block_size = 0,
    webassembly_udf_input_split_memory_ratio = 0;

DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
