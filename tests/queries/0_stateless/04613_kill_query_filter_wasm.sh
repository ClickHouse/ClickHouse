#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY works for WASM UDF in WHERE clause, covering the cancelExecution path
# inside a single long-running function (UserDefinedWebAssembly).
# Uses infinite_loop from faulty.wasm with unlimited fuel, then KILL QUERY.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_filter_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"

# Ensure isolation from earlier tests (03207_wasm_fault.sh) that also insert the faulty module
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty'"

# Load the WASM module with the infinite_loop function
cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty', code FROM input('code String') FORMAT RawBlob"

# Create the infinite_loop function
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE OR REPLACE FUNCTION infinite_loop LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty' ARGUMENTS (UInt32) RETURNS UInt32;
"

# Start a query that uses infinite_loop in WHERE with unlimited fuel
# (so it keeps running until cancelled via cancelExecution)
# Use max_execution_time=60 as a safety fallback
${CLICKHOUSE_CLIENT} --query_id="$query_id" --allow_experimental_analyzer=1 --query "
    SELECT count()
    FROM numbers(100000000)
    WHERE infinite_loop(1 :: UInt32) = 1
    FORMAT Null
    SETTINGS webassembly_udf_max_fuel = 0, max_execution_time = 60, max_threads = 1, max_block_size = 10000000
" >/dev/null 2>&1 &

# Give the query time to start executing the WASM function
sleep 2

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on the WASM function
# cancelExecution calls interrupt_source.request_stop(), which the WasmEdge runtime checks
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

wait

# Clean up
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty'"

echo "OK"
