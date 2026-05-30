#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
DROP VIEW IF EXISTS test_04299_wasm_mv;
DROP TABLE IF EXISTS test_04299_wasm_mv_dst;
DROP TABLE IF EXISTS test_04299_wasm_mv_src;
DROP FUNCTION IF EXISTS test_04299_wasm_identity;
DELETE FROM system.webassembly_modules WHERE name = 'test_04299_module';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'test_04299_module', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
CREATE FUNCTION test_04299_wasm_identity
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'test_04299_module' :: 'identity_raw'
    ARGUMENTS (x Int32) RETURNS Int32;
EOF

${CLICKHOUSE_CLIENT} << EOF
CREATE TABLE test_04299_wasm_mv_src (value Int32) ENGINE = Memory;
CREATE TABLE test_04299_wasm_mv_dst (value Int32) ENGINE = Memory;

CREATE MATERIALIZED VIEW test_04299_wasm_mv TO test_04299_wasm_mv_dst
AS SELECT test_04299_wasm_identity(value) AS value
FROM test_04299_wasm_mv_src;

INSERT INTO test_04299_wasm_mv_src VALUES (1), (2), (3);
SELECT groupArray(value) FROM (SELECT value FROM test_04299_wasm_mv_dst ORDER BY value);
EOF

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
DROP VIEW test_04299_wasm_mv;
DROP TABLE test_04299_wasm_mv_dst;
DROP TABLE test_04299_wasm_mv_src;
DROP FUNCTION test_04299_wasm_identity;
DELETE FROM system.webassembly_modules WHERE name = 'test_04299_module';
EOF
