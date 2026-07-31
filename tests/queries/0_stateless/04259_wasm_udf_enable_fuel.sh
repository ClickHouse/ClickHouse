#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
DROP FUNCTION IF EXISTS fuel_fib;
DROP FUNCTION IF EXISTS fuel_fib_no_fuel;
DROP FUNCTION IF EXISTS fuel_fib_bad;
DROP FUNCTION IF EXISTS fuel_infinite_loop;
DROP FUNCTION IF EXISTS fuel_infinite_loop_no_fuel;
DROP FUNCTION IF EXISTS fuel_start_add1;
DROP FUNCTION IF EXISTS fuel_start_add1_no_fuel;
DELETE FROM system.webassembly_modules WHERE name = 'faulty_fuel_toggle';
DELETE FROM system.webassembly_modules WHERE name = 'as_infinite_start_fuel_toggle';
EOF

cat "${CUR_DIR}/wasm/faulty.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_fuel_toggle', code FROM input('code String') FORMAT RawBlob"
cat "${CUR_DIR}/wasm/as_infinite_start.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'as_infinite_start_fuel_toggle', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION fuel_fib
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32;

SELECT fuel_fib(20 :: UInt32)
SETTINGS webassembly_udf_max_fuel = 1; -- { serverError WASM_ERROR }

CREATE OR REPLACE FUNCTION fuel_fib_no_fuel
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = false;

SELECT fuel_fib_no_fuel(20 :: UInt32)
SETTINGS webassembly_udf_max_fuel = 1;

SELECT fuel_fib(5 :: UInt32), fuel_fib_no_fuel(20 :: UInt32)
SETTINGS webassembly_udf_max_fuel = 0
FORMAT Null;

SELECT throwIf(count() != 1 OR countIf(match(create_query, 'webassembly_udf_enable_fuel\\s*=\\s*(false|0)')) != 1)
FROM system.functions
WHERE name = 'fuel_fib_no_fuel'
FORMAT Null;

CREATE OR REPLACE FUNCTION fuel_fib_bad
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = 2; -- { serverError BAD_ARGUMENTS }

CREATE OR REPLACE FUNCTION fuel_fib_bad
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = -1; -- { serverError BAD_ARGUMENTS }

CREATE OR REPLACE FUNCTION fuel_fib_bad
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = 'yes'; -- { serverError BAD_ARGUMENTS }

CREATE OR REPLACE FUNCTION fuel_fib_bad
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = 1.5; -- { serverError BAD_ARGUMENTS }

CREATE OR REPLACE FUNCTION fuel_fib_bad
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'fib'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = NULL; -- { serverError BAD_ARGUMENTS }

CREATE OR REPLACE FUNCTION fuel_infinite_loop_no_fuel
    LANGUAGE WASM ABI ROW_DIRECT
    FROM 'faulty_fuel_toggle' :: 'infinite_loop'
    ARGUMENTS (UInt32) RETURNS UInt32
    SETTINGS webassembly_udf_enable_fuel = false;

SELECT fuel_infinite_loop_no_fuel(1 :: UInt32)
SETTINGS max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED, WASM_ERROR }

CREATE OR REPLACE FUNCTION fuel_start_add1_no_fuel
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_infinite_start_fuel_toggle' :: 'add1'
    ARGUMENTS (a UInt64) RETURNS UInt64
    SETTINGS webassembly_udf_enable_fuel = false;

SELECT fuel_start_add1_no_fuel(1 :: UInt64)
SETTINGS max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED, WASM_ERROR }

DROP FUNCTION IF EXISTS fuel_fib;
DROP FUNCTION IF EXISTS fuel_fib_no_fuel;
DROP FUNCTION IF EXISTS fuel_fib_bad;
DROP FUNCTION IF EXISTS fuel_infinite_loop;
DROP FUNCTION IF EXISTS fuel_infinite_loop_no_fuel;
DROP FUNCTION IF EXISTS fuel_start_add1;
DROP FUNCTION IF EXISTS fuel_start_add1_no_fuel;
DELETE FROM system.webassembly_modules WHERE name = 'faulty_fuel_toggle';
DELETE FROM system.webassembly_modules WHERE name = 'as_infinite_start_fuel_toggle';
EOF
