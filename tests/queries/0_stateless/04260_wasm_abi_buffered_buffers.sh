#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
DROP FUNCTION IF EXISTS sum_buffers;
DROP FUNCTION IF EXISTS bad_buffers_zero_columns;
DROP FUNCTION IF EXISTS bad_buffers_wrong_size;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_buffers';
EOF

cat "${CUR_DIR}/wasm/buffered_abi.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'buffered_abi_buffers', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION sum_buffers
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'buffered_abi_buffers' :: 'sum_buffers_u32_u64'
    ARGUMENTS (a UInt32, b UInt64) RETURNS UInt64
    SETTINGS serialization_format = 'Buffers';

SELECT sum_buffers(toUInt32(number), toUInt64(number * 10))
FROM numbers(10)
ORDER BY number
SETTINGS max_block_size = 4, webassembly_udf_max_input_block_size = 3;

SELECT sum(sum_buffers(toUInt32(number), toUInt64(number * 10)))
FROM numbers(100)
SETTINGS max_block_size = 17, webassembly_udf_max_input_block_size = 7;

CREATE OR REPLACE FUNCTION bad_buffers_zero_columns
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'buffered_abi_buffers' :: 'malformed_buffers_zero_columns'
    ARGUMENTS (a UInt32) RETURNS UInt64
    SETTINGS serialization_format = 'Buffers';

SELECT bad_buffers_zero_columns(toUInt32(number))
FROM numbers(1)
SETTINGS webassembly_udf_max_input_block_size = 1; -- { serverError INCORRECT_DATA }

CREATE OR REPLACE FUNCTION bad_buffers_wrong_size
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'buffered_abi_buffers' :: 'malformed_buffers_wrong_size'
    ARGUMENTS (a UInt32) RETURNS UInt64
    SETTINGS serialization_format = 'Buffers';

-- The guest writes a fixed `num_rows = 1` envelope; the test must invoke it with one row.
-- `FROM numbers(1)` plus `webassembly_udf_max_input_block_size = 1` guarantees `n = 1`.
SELECT bad_buffers_wrong_size(toUInt32(number))
FROM numbers(1)
SETTINGS webassembly_udf_max_input_block_size = 1; -- { serverError INCORRECT_DATA }

DROP FUNCTION IF EXISTS sum_buffers;
DROP FUNCTION IF EXISTS bad_buffers_zero_columns;
DROP FUNCTION IF EXISTS bad_buffers_wrong_size;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_buffers';
EOF
