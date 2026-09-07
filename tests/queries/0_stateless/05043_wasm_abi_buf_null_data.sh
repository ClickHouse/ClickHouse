#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << EOF

DROP FUNCTION IF EXISTS returns_null_data_pointer;
DROP FUNCTION IF EXISTS null_data_test_func;

DELETE FROM system.webassembly_modules WHERE name = 'abi_buf_null_data';

EOF

cat "${CUR_DIR}"/wasm/abi_buf_null_data.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'abi_buf_null_data', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "CREATE OR REPLACE FUNCTION returns_null_data_pointer LANGUAGE WASM ABI BUFFERED_V1 FROM 'abi_buf_null_data' ARGUMENTS (UInt32) RETURNS Int32"

# A buffer with a non-zero size must have a non-zero data pointer, the host rejects such a buffer.
# Check both the error class and the message: the query must fail with `WASM_ERROR`, and it must fail
# because of the null data pointer, not for some other reason.
${CLICKHOUSE_CLIENT} --query "SELECT returns_null_data_pointer(0 :: UInt32) -- { serverError WASM_ERROR }"
${CLICKHOUSE_CLIENT} --query "SELECT returns_null_data_pointer(0 :: UInt32)" 2>&1 | grep -o -m1 "returned null data pointer with size 42"

${CLICKHOUSE_CLIENT} << EOF

-- The module itself is ok, a properly defined function from it still works.
CREATE OR REPLACE FUNCTION null_data_test_func LANGUAGE WASM ABI BUFFERED_V1 FROM 'abi_buf_null_data' ARGUMENTS (UInt64) RETURNS UInt64 SETTINGS serialization_format = 'CSV';
SELECT null_data_test_func(456 :: UInt64), null_data_test_func(materialize(521 :: UInt64));

DROP FUNCTION returns_null_data_pointer;
DROP FUNCTION null_data_test_func;
DELETE FROM system.webassembly_modules WHERE name = 'abi_buf_null_data';

EOF
