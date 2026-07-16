#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS returns_out_of_bounds;
DROP FUNCTION IF EXISTS returns_out_of_bounds2;
DROP FUNCTION IF EXISTS test_func;

DELETE FROM system.webassembly_modules WHERE name = 'abi_buf_out_of_bounds';

EOF

cat ${CUR_DIR}/wasm/abi_buf_out_of_bounds.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'abi_buf_out_of_bounds', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION returns_out_of_bounds LANGUAGE WASM ABI BUFFERED_V1 FROM 'abi_buf_out_of_bounds' ARGUMENTS (UInt32) RETURNS Int32;
SELECT returns_out_of_bounds(0 :: UInt32); -- { serverError WASM_ERROR }

CREATE OR REPLACE FUNCTION returns_out_of_bounds2 LANGUAGE WASM ABI BUFFERED_V1 FROM 'abi_buf_out_of_bounds' ARGUMENTS (UInt32) RETURNS Int32;
SELECT returns_out_of_bounds2(0 :: UInt32); -- { serverError WASM_ERROR }

-- module itself is ok, test properly defined function from it
CREATE OR REPLACE FUNCTION test_func LANGUAGE WASM ABI BUFFERED_V1 FROM 'abi_buf_out_of_bounds' ARGUMENTS (UInt64) RETURNS UInt64 SETTINGS serialization_format = 'CSV';
SELECT test_func(456 :: UInt64), test_func(materialize(521 :: UInt64));

EOF
