#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS huge_allocate;
DROP FUNCTION IF EXISTS infinite_loop;
DROP FUNCTION IF EXISTS fib_wasm;
DROP FUNCTION IF EXISTS write_out_of_bounds;
DROP FUNCTION IF EXISTS read_out_of_bounds;

DELETE FROM system.webassembly_modules WHERE name = 'faulty';

EOF

cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

-- this function tries to grow number of pages specified in the argument
-- and stops on unsuccessful allocation returning the number of successfully allocated pages

CREATE OR REPLACE FUNCTION huge_allocate LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT huge_allocate(1 :: UInt32) <= 10 SETTINGS webassembly_udf_max_memory = 655360;
SELECT huge_allocate(10 :: UInt32) == 0 SETTINGS webassembly_udf_max_memory = 655360;
SELECT 10 < huge_allocate(1 :: UInt32) AND huge_allocate(1 :: UInt32) <= 100 SETTINGS webassembly_udf_max_memory = 6553600;

CREATE OR REPLACE FUNCTION infinite_loop LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT infinite_loop(1 :: UInt32); -- { serverError WASM_ERROR }
-- Fuel is unlimited, but query still can be cancelled by timeout
SELECT infinite_loop(1 :: UInt32) SETTINGS webassembly_udf_max_fuel = 18446744073709551615, max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
SELECT infinite_loop(1 :: UInt32) SETTINGS webassembly_udf_max_fuel = 0, max_execution_time = 0.5; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

CREATE OR REPLACE FUNCTION fib_wasm LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' :: 'fib' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT fib_wasm(5 :: UInt32);

SELECT fib_wasm((100 * number + 1) :: UInt32) FROM numbers(100000); -- { serverError WASM_ERROR }
SELECT fib_wasm(0 :: UInt32); -- { serverError WASM_ERROR }

DROP FUNCTION IF EXISTS fib_wasm;

CREATE OR REPLACE FUNCTION write_out_of_bounds LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT write_out_of_bounds(number:: UInt32) FROM numbers(10); -- { serverError WASM_ERROR }
DROP FUNCTION IF EXISTS write_out_of_bounds;

CREATE OR REPLACE FUNCTION read_out_of_bounds LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT read_out_of_bounds(number:: UInt32) FROM numbers(10); -- { serverError WASM_ERROR }
DROP FUNCTION IF EXISTS read_out_of_bounds;

DROP FUNCTION IF EXISTS huge_allocate;
DROP FUNCTION IF EXISTS infinite_loop;

EOF
