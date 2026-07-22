#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY works for WASM UDF in WHERE clause, covering the cancelExecution path
# inside a single long-running function (UserDefinedWebAssembly).
# Uses infinite_loop_04613 from faulty.wasm (unique name for isolation from 03207_wasm_fault.sh) with unlimited fuel, then KILL QUERY.
# Asserts QUERY_WAS_CANCELLED — if the test hangs, cancellation is broken.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_filter_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_filter_wasm_${CLICKHOUSE_DATABASE}.out"

# Use module/function names unique to 04613 for isolation from 03207_wasm_fault.sh.
# Clean up any stale entry from a previous failed 04613 run.
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04613'"

# Load the WASM module with the infinite_loop function
cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_04613', code FROM input('code String') FORMAT RawBlob"

# Create the infinite_loop_04613 function
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE OR REPLACE FUNCTION infinite_loop_04613 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_04613' ARGUMENTS (UInt32) RETURNS UInt32;
"

# Start a query that uses infinite_loop in WHERE with unlimited fuel
# (so it keeps running until cancelled via cancelExecution)
# No max_execution_time: if KILL doesn't work the test hangs (fails loudly).
${CLICKHOUSE_CLIENT} --query_id="$query_id" --allow_experimental_analyzer=1 --query "
    SELECT count()
    FROM numbers(100000000)
    WHERE infinite_loop_04613(1 :: UInt32) = 1
    FORMAT Null
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1, max_block_size = 10000000, max_rows_to_read = 0
" >"$output_file" 2>&1 &

# Wait for the query to start executing the WASM function
for _ in $(seq 1 300); do
    [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" == "1" ]] && break
    sleep 0.1
done

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on the WASM function
# cancelExecution calls interrupt_source.request_stop(), which the WasmEdge runtime checks
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

wait

# Assert cancellation was detected, not timeout
grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; exit 1; }

# Clean up
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04613"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04613'"

echo "OK"
