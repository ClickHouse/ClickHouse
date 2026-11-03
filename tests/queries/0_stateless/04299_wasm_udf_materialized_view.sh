#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

module_name="test_04299_module_${CLICKHOUSE_TEST_UNIQUE_NAME}"
replacement_module_name="test_04299_replacement_module_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function_name="test_04299_wasm_identity_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
DROP VIEW IF EXISTS test_04299_wasm_mv;
DROP TABLE IF EXISTS test_04299_wasm_mv_dst;
DROP TABLE IF EXISTS test_04299_wasm_mv_src;
DROP FUNCTION IF EXISTS ${function_name};
DELETE FROM system.webassembly_modules WHERE name = '${module_name}';
DELETE FROM system.webassembly_modules WHERE name = '${replacement_module_name}';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${module_name}', code FROM input('code String') FORMAT RawBlob"
cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${replacement_module_name}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
CREATE FUNCTION ${function_name}
    LANGUAGE WASM ABI ROW_DIRECT
    FROM '${module_name}' :: 'identity_raw'
    ARGUMENTS (x Int32) RETURNS Int32;

CREATE FUNCTION IF NOT EXISTS ${function_name}
    LANGUAGE WASM ABI ROW_DIRECT
    FROM '${replacement_module_name}' :: 'fib'
    ARGUMENTS (x Int32) RETURNS Int32;
EOF

${CLICKHOUSE_CLIENT} << EOF
SELECT ${function_name}(4);

CREATE TABLE test_04299_wasm_mv_src (value Int32) ENGINE = Memory;
CREATE TABLE test_04299_wasm_mv_dst (value Int32) ENGINE = Memory;

CREATE MATERIALIZED VIEW test_04299_wasm_mv TO test_04299_wasm_mv_dst
AS SELECT ${function_name}(value) AS value
FROM test_04299_wasm_mv_src;

INSERT INTO test_04299_wasm_mv_src VALUES (1), (2), (3);
SELECT groupArray(value) FROM (SELECT value FROM test_04299_wasm_mv_dst ORDER BY value);
EOF

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
DROP VIEW test_04299_wasm_mv;
DROP TABLE test_04299_wasm_mv_dst;
DROP TABLE test_04299_wasm_mv_src;
DROP FUNCTION ${function_name};
DELETE FROM system.webassembly_modules WHERE name = '${module_name}';
DELETE FROM system.webassembly_modules WHERE name = '${replacement_module_name}';
EOF
