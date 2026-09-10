#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# WebAssembly modules and functions are server-wide, so the names carry the database to keep
# concurrent runs of this test apart.
MODULE="batch_csv_abi_${CLICKHOUSE_DATABASE}"
FUNC="wasm_batch_csv_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/batch_size_csv_abi.wasm

# `get_block_size_csv` reports, once per row, how many rows the host put in the batch that row
# belonged to, over a `CSV` wire.
#
# One very wide row sitting deep in an otherwise narrow block is the case the extrapolation is
# worst at: the two measurements the slope is read from straddle that row, so the slope is the
# width of the outlier rather than of the rows around it, and the walk crawls up from the bottom
# of the bracket instead of shrinking it. Every probe the walk spends this way is a batch boundary
# placed short of where the budget allows.
#
# The module is linked with an explicit initial linear memory of 1048576 bytes, so the budget is
# an exact number: 1048576 * 0.002 = 2097 bytes. A `CSV` row of one `String` column costs the
# quoted value plus a newline, so a row of 'a' costs 4 bytes and 524 of them is the largest batch
# the budget holds. The block below is 1000 narrow rows, one 50000-character row, and four more
# narrow rows, so the batches the budget allows are 524, 476, 1 and 4 - four calls in all. Before
# the midpoint fallback the walk produced eight, each one shorter than the budget allowed.

${CLICKHOUSE_CLIENT} --query "

SET webassembly_udf_max_fuel = 100000000;
SET max_threads = 1;
-- One block for the whole query, so the batches below are cut by the budget alone.
SET max_block_size = 2000;
-- Split by size rather than by a fixed row count.
SET webassembly_udf_max_input_block_size = 0;
SET webassembly_udf_input_split_memory_ratio = 0.002;

CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1 FROM '${MODULE}' :: 'get_block_size_csv'
    ARGUMENTS (value String) RETURNS UInt64
    SETTINGS serialization_format = 'CSV';

SELECT 'a late wide row does not shrink the batches before it', max(v) = 524, min(v) = 1, uniqExact(v) = 4, count() = 1005
FROM (SELECT ${FUNC}(s) AS v FROM (SELECT if(number = 1000, repeat('a', 50000), 'a') AS s FROM numbers(1005)));

DROP FUNCTION ${FUNC};
"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
