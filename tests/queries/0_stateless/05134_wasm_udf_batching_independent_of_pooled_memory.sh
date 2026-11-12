#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Compartments are pooled and WebAssembly memory never shrinks after `memory.grow`, so a budget
# taken from the current linear memory would make the batching depend on which instance a worker
# picked up and on what earlier blocks grew it to. The guest sees the row count of each call, so
# identical blocks must reach it identically.

MODULE="pooled_${CLICKHOUSE_DATABASE}"
FUNC="wasm_pooled_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/growable_zero_page_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS (s String) RETURNS Array(UInt32)
    SETTINGS serialization_format = 'CSV'"

# Several identical blocks in one query: the first grows the compartment, the rest must still be
# cut into the same batches.
echo "identical blocks are batched identically whatever the pooled memory grew to"
${CLICKHOUSE_CLIENT} --query "
SELECT uniqExact(widest_batch) FROM (
    SELECT max(batch_rows) AS widest_batch
    FROM (SELECT blockNumber() AS block, ${FUNC}(materialize(repeat('a', 10000)))[1] AS batch_rows FROM numbers(256))
    GROUP BY block
) SETTINGS max_block_size = 64, max_threads = 1"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
