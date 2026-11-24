#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# WebAssembly modules and functions are server-wide, so the names carry the database to keep
# concurrent runs of this test apart.
MODULE="batch_buffers_abi_${CLICKHOUSE_DATABASE}"
FUNC="wasm_batch_buffers_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/batch_size_buffers_abi.wasm

# `get_block_size_buffers` reports, once per row, how many rows the host put in the batch that row
# belonged to, over a `Buffers` wire.
#
# `Buffers` is block-scoped: a block of one `UInt64` column costs 24 bytes of framing - column
# count, row count, column size - plus 8 bytes per row, and the framing is paid once no matter how
# many rows follow. That is what makes it the wire this test needs. Under `CSV` a batch costs
# exactly the sum of its rows, so measuring the batch whole and summing one-row probes agree; here
# they do not, and the row counts below say which one the splitter did.
#
# The module is linked with an explicit initial linear memory of 1048576 bytes, so the budget is an
# exact number: 1048576 * 0.01 = 10485 bytes. Measuring the batch whole, the largest batch that
# fits is 24 + 8 * 1307 = 10480 bytes, one row short of 24 + 8 * 1308 = 10488. Summing one-row
# probes charges the 24 bytes of framing to every row and stops at 10485 / 32 = 327.

${CLICKHOUSE_CLIENT} --query "

SET webassembly_udf_max_fuel = 100000000;
SET max_threads = 1;
-- One block for the whole query, so the batches below are cut by the budget alone.
SET max_block_size = 4000;
-- Split by size rather than by a fixed row count.
SET webassembly_udf_max_input_block_size = 0;
SET webassembly_udf_input_split_memory_ratio = 0.01;

CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1 FROM '${MODULE}' :: 'get_block_size_buffers'
    ARGUMENTS (value UInt64) RETURNS UInt64
    SETTINGS serialization_format = 'Buffers';

-- 4000 rows fall into batches of 1307, 1307, 1307 and a remainder of 79, so rows 0..3920 report
-- 1307 and rows 3921..3999 report 79. Every row carries the size of its own batch, and batches are
-- contiguous, so the per-row expectation below pins each boundary rather than only the extremes.
SELECT 'block-scoped framing is charged once per batch',
       countIf(v != if(number < 3921, 1307, 79)) = 0,
       count() = 4000
FROM (SELECT number, ${FUNC}(number) AS v FROM numbers(4000));

DROP FUNCTION ${FUNC};
"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
