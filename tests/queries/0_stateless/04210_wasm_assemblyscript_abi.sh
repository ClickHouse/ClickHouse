#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_analyzer=1 <<EOF
DROP FUNCTION IF EXISTS as_add;
DROP FUNCTION IF EXISTS as_double;
DROP FUNCTION IF EXISTS as_greet;
DROP FUNCTION IF EXISTS as_str_repeat;
DROP FUNCTION IF EXISTS as_str_length;
DROP FUNCTION IF EXISTS as_concat3;
DROP FUNCTION IF EXISTS as_add128;
DROP FUNCTION IF EXISTS as_start_add1;
DELETE FROM system.webassembly_modules WHERE name = 'as_example';
DELETE FROM system.webassembly_modules WHERE name = 'as_infinite_start';
EOF

cat "${CUR_DIR}/wasm/as_example.wasm" | ${CLICKHOUSE_CLIENT} --enable_analyzer=1 --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'as_example', code FROM input('code String') FORMAT RawBlob"
cat "${CUR_DIR}/wasm/as_infinite_start.wasm" | ${CLICKHOUSE_CLIENT} --enable_analyzer=1 --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'as_infinite_start', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 <<EOF
SET webassembly_udf_max_fuel = 100000000;

CREATE OR REPLACE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE OR REPLACE FUNCTION as_double
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'as_double'
    ARGUMENTS (x Float64) RETURNS Float64;

CREATE OR REPLACE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;

CREATE OR REPLACE FUNCTION as_str_repeat
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'str_repeat'
    ARGUMENTS (s String, n UInt32) RETURNS String;

CREATE OR REPLACE FUNCTION as_str_length
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'str_length'
    ARGUMENTS (s String) RETURNS UInt32;

CREATE OR REPLACE FUNCTION as_concat3
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'concat3'
    ARGUMENTS (a String, b String, c String) RETURNS String;

CREATE OR REPLACE FUNCTION as_add128
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add128'
    ARGUMENTS (a UInt128, b UInt128) RETURNS UInt128;

SELECT '== numeric ==';
SELECT as_add(1::UInt32, 2::UInt32);
SELECT sum(as_add(number::UInt32, (number*2)::UInt32)) FROM numbers(10);
SELECT as_double(3.5);

SELECT '== strings ==';
SELECT as_greet('world');
SELECT as_greet(arrayJoin(['Alice', 'Bob', 'Привет']));
SELECT as_str_repeat('ab', 3::UInt32);
SELECT as_str_length('café');
SELECT as_str_length('🦀');

SELECT '== nested ==';
SELECT as_str_length(as_greet(arrayJoin(['x', 'yz'])));

SELECT '== block ==';
SELECT count(), sum(length(as_greet(toString(number)))) FROM numbers(1000);

SELECT '== multi-string ==';
SELECT as_concat3('hello', 'world', '!');
SELECT as_concat3(toString(number), toString(number + 1), toString(number + 2)) FROM numbers(3);
-- Stress the AS GC: many rows, each row allocates three sibling AS String objects.
-- Without pinning, a later __new can collect an earlier argument before invocation,
-- producing a wrong/garbled result or a WASM_ERROR exception.
SELECT count(), sum(length(as_concat3(repeat('a', 64), repeat('b', 64), repeat('c', 64))))
FROM numbers(2000);
SELECT count(DISTINCT as_concat3(toString(number), toString(number * 7), toString(number * 13)))
FROM numbers(500);

SELECT '== v128 ==';
SELECT as_add128(toUInt128(1), toUInt128(2));
-- Carry from low into high: (2^64 - 1) + 1 == 2^64.
SELECT as_add128(toUInt128(toUInt64(-1)), toUInt128(1));
-- Wrap at 2^128: max UInt128 + 1 == 0.
SELECT as_add128(toUInt128('340282366920938463463374607431768211455'), toUInt128(1));
-- Two high-half operands sum cleanly into the high lane.
SELECT as_add128(toUInt128('18446744073709551616') /* 2^64 */, toUInt128('18446744073709551616'));
SELECT sum(as_add128(toUInt128(number), toUInt128(number))) FROM numbers(100);

-- Module with infinite loop in start function, to test interruption
CREATE OR REPLACE FUNCTION as_start_add1
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_infinite_start' :: 'add1'
    ARGUMENTS (a UInt64) RETURNS UInt64;

SELECT as_start_add1(1 :: UInt32) SETTINGS webassembly_udf_max_fuel = 18446744073709551615, max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
SELECT as_start_add1(1 :: UInt32) SETTINGS webassembly_udf_max_fuel = 0, max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

DROP FUNCTION as_add;
DROP FUNCTION as_double;
DROP FUNCTION as_greet;
DROP FUNCTION as_str_repeat;
DROP FUNCTION as_str_length;
DROP FUNCTION as_concat3;
DROP FUNCTION as_add128;
DROP FUNCTION as_start_add1;
DELETE FROM system.webassembly_modules WHERE name = 'as_example';
DELETE FROM system.webassembly_modules WHERE name = 'as_infinite_start';
EOF
