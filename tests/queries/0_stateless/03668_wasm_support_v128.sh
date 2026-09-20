#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS concatInts;
DELETE FROM system.webassembly_modules WHERE name = 'test_v128';

EOF

cat ${CUR_DIR}/wasm/test_v128.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'test_v128', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION concatInts LANGUAGE WASM ABI ROW_DIRECT FROM 'test_v128' ARGUMENTS (UInt64, UInt64) RETURNS UInt128;
SELECT concatInts(1 :: UInt64, 0 :: UInt64);
DROP FUNCTION IF EXISTS concatInts;

EOF
