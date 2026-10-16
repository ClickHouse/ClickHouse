#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `webassembly_udf_max_input_block_size` pins the rows per call, and the split memory ratio is
# never read on that path, so an out-of-range ratio must not fail the query there. It is still
# rejected where it is used, with dynamic splitting sizing the batches.

MODULE="fixed_ratio_${CLICKHOUSE_DATABASE}"
FUNC="wasm_fixed_ratio_${CLICKHOUSE_DATABASE}"

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

echo "a pinned row count ignores an out-of-range ratio"
${CLICKHOUSE_CLIENT} --query "
SELECT sum(${FUNC}(materialize('a'))[1]) FROM numbers(4)
SETTINGS max_threads = 1, webassembly_udf_max_input_block_size = 1, webassembly_udf_input_split_memory_ratio = 2"

echo "the ratio is still rejected where it decides the batch size"
${CLICKHOUSE_CLIENT} --query "
SELECT sum(${FUNC}(materialize('a'))[1]) FROM numbers(4)
SETTINGS max_threads = 1, webassembly_udf_max_input_block_size = 0, webassembly_udf_input_split_memory_ratio = 2" 2>&1 \
  | grep -c -m 1 "must be at least 0 and at most 1"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
