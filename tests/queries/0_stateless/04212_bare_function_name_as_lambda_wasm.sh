#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Reason for no-parallel: this test creates global WASM UDFs and a global
# `system.webassembly_modules` row; concurrent runs in flaky check would race
# on `CREATE FUNCTION`, `DROP FUNCTION`, and module load/delete.
#
# Test passing bare WASM UDF names to higher-order functions.
# Companion to `04064_bare_function_name_as_lambda.sql` for `LANGUAGE WASM`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
DROP FUNCTION IF EXISTS test_04212_wasm_identity;
DROP FUNCTION IF EXISTS test_04212_wasm_add;
DELETE FROM system.webassembly_modules WHERE name = 'test_04212_module';
EOF

cat ${CUR_DIR}/wasm/identity_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'test_04212_module', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << EOF
-- Unary WASM UDF: identity over Int32.
CREATE FUNCTION test_04212_wasm_identity
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'test_04212_module' :: 'identity_raw'
    ARGUMENTS (x Int32) RETURNS Int32;

-- Binary WASM UDF: i32 + i32 -> i32.
CREATE FUNCTION test_04212_wasm_add
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'test_04212_module' :: 'add'
    ARGUMENTS (x Int32, y Int32) RETURNS Int32;

-- Bare unary WASM UDF passed to arrayMap.
SELECT arrayMap(test_04212_wasm_identity, [toInt32(1), toInt32(2), toInt32(3)]);

-- Equivalent explicit lambda must produce the same result.
SELECT arrayMap(x -> test_04212_wasm_identity(x), [toInt32(1), toInt32(2), toInt32(3)]);

-- Bare binary WASM UDF with two array arguments.
SELECT arrayMap(test_04212_wasm_add, [toInt32(1), toInt32(2), toInt32(3)], [toInt32(10), toInt32(20), toInt32(30)]);

-- Tuple-destructuring: declared arity 2 lets the rewrite produce
-- a binary lambda over a single Array(Tuple(...)).
SELECT arrayMap(test_04212_wasm_add, [(toInt32(1), toInt32(10)), (toInt32(2), toInt32(20)), (toInt32(3), toInt32(30))]);

-- Bare WASM UDF in arrayFold (accumulator + element).
SELECT arrayFold(test_04212_wasm_add, [toInt32(1), toInt32(2), toInt32(3), toInt32(4), toInt32(5)], toInt32(0));

DROP FUNCTION test_04212_wasm_identity;
DROP FUNCTION test_04212_wasm_add;
DELETE FROM system.webassembly_modules WHERE name = 'test_04212_module';
EOF
