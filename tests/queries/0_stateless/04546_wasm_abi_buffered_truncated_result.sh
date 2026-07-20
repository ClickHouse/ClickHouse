#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

# Regression test: a BUFFERED_V1 WebAssembly function that returns a non-empty
# but truncated result buffer (shorter than one full RowBinary row) makes the
# host-side parser fail mid-row. That failure must surface as a catchable
# WASM_ERROR, not as a low-level read error leaking from the input format.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
DROP FUNCTION IF EXISTS wasm_returns_truncated;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_truncated';
EOF

cat "${CUR_DIR}/wasm/buffered_abi.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'buffered_abi_truncated', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION wasm_returns_truncated
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'buffered_abi_truncated' :: 'returns_truncated_buffer'
    ARGUMENTS (a UInt32) RETURNS UInt64
    SETTINGS serialization_format = 'RowBinary';

SELECT wasm_returns_truncated(toUInt32(number))
FROM numbers(1)
SETTINGS webassembly_udf_max_input_block_size = 1; -- { serverError WASM_ERROR }

DROP FUNCTION IF EXISTS wasm_returns_truncated;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_truncated';
EOF
