#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SettingsChangesHistory` promises that `compatibility` below 26.9 restores the previous
# behaviour, where `webassembly_udf_max_input_block_size = 0` meant one call per pipeline block.
# The guest sees the batch size as `num_rows`, so the rollback path is user-visible, and a wrong
# version block or wrong previous value in the history entry would break it silently.

MODULE="splitting_compat_${CLICKHOUSE_DATABASE}"
FUNC="wasm_splitting_compat_${CLICKHOUSE_DATABASE}"

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

SELECT 'compatibility below 26.9: one call per pipeline block';
SELECT max(batch_rows) AS batch_rows
FROM
(
    SELECT ${FUNC}(repeat('a', 200))[1] AS batch_rows
    FROM numbers(4096)
)
SETTINGS max_block_size = 4096, max_threads = 1, webassembly_udf_max_input_block_size = 0,
    compatibility = '26.8';

DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
