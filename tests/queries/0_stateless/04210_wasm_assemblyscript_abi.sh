#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} <<EOF
DROP FUNCTION IF EXISTS as_add;
DROP FUNCTION IF EXISTS as_double;
DROP FUNCTION IF EXISTS as_greet;
DROP FUNCTION IF EXISTS as_str_repeat;
DROP FUNCTION IF EXISTS as_str_length;
DELETE FROM system.webassembly_modules WHERE name = 'as_example';
EOF

cat "${CUR_DIR}/wasm/as_example.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'as_example', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} <<EOF
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

DROP FUNCTION as_add;
DROP FUNCTION as_double;
DROP FUNCTION as_greet;
DROP FUNCTION as_str_repeat;
DROP FUNCTION as_str_length;
DELETE FROM system.webassembly_modules WHERE name = 'as_example';
EOF
