#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `webassembly_udf_max_memory = 0` means "no host cap", and a module that declares no
# `memory.maximum` puts no cap of its own on growth either. Nothing bounds the guest's linear
# memory in that configuration except the 4 GiB a `wasm32` module can address, and the splitter
# must still work: it sizes a call against the memory the module declares it starts with, never
# against the memory a pooled instance happens to have grown to for earlier blocks, which the
# guest would observe through the row count.
#
# Each row here is a 100000-character `String`, whose CSV rendering is ~100 KiB.

MODULE="unbounded_memory_${CLICKHOUSE_DATABASE}"
FUNC="wasm_unbounded_memory_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (x String) RETURNS UInt32
    SETTINGS serialization_format = 'CSV';

-- \`max(batch_rows)\` keeps the UDF call alive: selecting only \`count()\` lets the analyzer
-- prune the unused column and the function is never executed at all.
SELECT 'wide row with no memory cap';
SELECT count() = 8 AS all_rows_processed, max(batch_rows) >= 1 AS batches_non_empty
FROM
(
    SELECT ${FUNC}(materialize(repeat('x', 100000))) AS batch_rows
    FROM numbers(8)
)
SETTINGS max_block_size = 8, max_threads = 1, webassembly_udf_max_input_block_size = 0, webassembly_udf_max_memory = 0;

DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
