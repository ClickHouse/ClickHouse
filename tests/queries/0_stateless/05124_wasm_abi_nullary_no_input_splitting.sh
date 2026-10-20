#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A nullary `BUFFERED_V1` function receives no input buffer at all: `executeOnBlock` skips
# serialization for an empty block and passes the guest a zero handle. Measuring that block
# anyway charges the call for bytes the guest never sees - `{}` under `JSONEachRow`, a bare
# newline under `CSV` - and lets them drive the splitting, which makes the row count a call
# observes depend on the `serialization_format` of an input that does not exist.

MODULE="nullary_split_${CLICKHOUSE_DATABASE}"
FUNC_JSON="wasm_nullary_json_${CLICKHOUSE_DATABASE}"
FUNC_CSV="wasm_nullary_csv_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP FUNCTION IF EXISTS ${FUNC_JSON};
DROP FUNCTION IF EXISTS ${FUNC_CSV};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE OR REPLACE FUNCTION ${FUNC_JSON}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count_json'
    ARGUMENTS () RETURNS Array(UInt32)
    SETTINGS serialization_format = 'JSONEachRow';

CREATE OR REPLACE FUNCTION ${FUNC_CSV}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'batch_row_count'
    ARGUMENTS () RETURNS UInt32
    SETTINGS serialization_format = 'CSV';"

# The whole pipeline block reaches the guest in one call, whatever the format of the input it
# never receives. The budget below is a few hundred bytes, so counting the phantom per-row
# bytes would chop the block into batches - and into differently sized ones per format.
SELECT_SETTINGS="max_block_size = 4096, max_threads = 1, webassembly_udf_max_input_block_size = 0, webassembly_udf_max_memory = 1179648, webassembly_udf_input_split_memory_ratio = 0.0001"

echo 'JSONEachRow'
${CLICKHOUSE_CLIENT} --query "
SELECT max(batch_rows) FROM (SELECT ${FUNC_JSON}()[1] AS batch_rows FROM numbers(4096))
SETTINGS ${SELECT_SETTINGS}"

echo 'CSV'
${CLICKHOUSE_CLIENT} --query "
SELECT max(batch_rows) FROM (SELECT ${FUNC_CSV}() AS batch_rows FROM numbers(4096))
SETTINGS ${SELECT_SETTINGS}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP FUNCTION IF EXISTS ${FUNC_JSON};
DROP FUNCTION IF EXISTS ${FUNC_CSV};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';"
