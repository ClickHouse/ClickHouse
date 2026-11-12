#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A WASM UDF is not deterministic unless declared so, so a constant argument reaches it as a
# `ColumnConst` spanning the whole block. Measuring the rows must not materialize that constant:
# expanding it to one copy per row costs far more than the batches the measurement is sizing, and
# would fail exactly the wide input that splitting exists to rescue. The memory limit below is
# well above what the batches need and well below the expanded constant.

MODULE="const_measure_${CLICKHOUSE_DATABASE}"
FUNC="wasm_const_measure_${CLICKHOUSE_DATABASE}"

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

echo 'a constant argument is measured without being expanded to one copy per row'
${CLICKHOUSE_CLIENT} --query "
SELECT max(batch_rows) > 0 AS split_into_batches
FROM
(
    SELECT ${FUNC}(repeat('a', 4096))[1] AS batch_rows
    FROM numbers(16384)
)
SETTINGS max_block_size = 16384, max_threads = 1, webassembly_udf_max_memory = 8388608, max_memory_usage = 40000000"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
