#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# WebAssembly modules and functions are server-wide, so the names carry the database to keep
# concurrent runs of this test apart.
MODULE="batch_widths_abi_${CLICKHOUSE_DATABASE}"
FUNC="wasm_batch_widths_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/buffered_abi.wasm

# `get_block_size` reports, once per row, how many rows the host put in the batch that row belonged to.
#
# A block whose first row is far wider than the rest is what tells apart a probe that brackets the
# real boundary from one that stops early. The first row alone already covers most of the budget, so
# a probe which accepts a nearly-full prefix, or which rescales by the average bytes per row of the
# candidate, concludes that no second row fits and hands the guest one row per call. The rows after it
# are narrow, and hundreds of them do fit beside it.

${CLICKHOUSE_CLIENT} --query "

SET webassembly_udf_max_fuel = 100000000;
SET max_threads = 1;
SET max_block_size = 1000;
-- Split by size rather than by a fixed row count, against a budget small enough to split a block.
-- The module declares 1026 initial pages, so this is a budget of about 6.7 KB.
SET webassembly_udf_max_input_block_size = 0;
SET webassembly_udf_input_split_memory_ratio = 0.0001;

CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1 FROM '${MODULE}' :: 'get_block_size'
    ARGUMENTS (value String) RETURNS UInt64
    SETTINGS serialization_format = 'CSV';

SELECT 'wide first row does not force a batch of one row', min(v) > 50, count() = 1000
FROM (SELECT ${FUNC}(s) AS v FROM (SELECT if(number = 0, repeat('a', 5500), 'x') AS s FROM numbers(1000)));

-- The same holds when the wide row sits in the middle: the batch that ends before it is bounded by
-- the budget, not by the first row it could not add.
SELECT 'wide row inside the block does not force a batch of one row', min(v) > 50, count() = 1000
FROM (SELECT ${FUNC}(s) AS v FROM (SELECT if(number = 500, repeat('a', 5500), 'x') AS s FROM numbers(1000)));

DROP FUNCTION ${FUNC};
"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
