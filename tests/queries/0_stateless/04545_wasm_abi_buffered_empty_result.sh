#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

# Regression test: a BUFFERED_V1 WebAssembly function that returns a zero-length
# buffer deserializes to zero rows under a row-based serialization_format. The
# host used to build an empty result column vector and then index past its end,
# aborting the server. It must now surface a WASM_ERROR instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
DROP FUNCTION IF EXISTS wasm_returns_empty;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_empty';
EOF

cat "${CUR_DIR}/wasm/buffered_abi.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'buffered_abi_empty', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION wasm_returns_empty
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'buffered_abi_empty' :: 'returns_empty_buffer'
    ARGUMENTS (a UInt32) RETURNS UInt64
    SETTINGS serialization_format = 'RowBinary';

SELECT wasm_returns_empty(toUInt32(number))
FROM numbers(1)
SETTINGS webassembly_udf_max_input_block_size = 1; -- { serverError WASM_ERROR }

DROP FUNCTION IF EXISTS wasm_returns_empty;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_empty';
EOF
