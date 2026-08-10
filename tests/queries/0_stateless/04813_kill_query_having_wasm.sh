#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY interrupts a WASM UDF that is still running inside a HAVING clause
# evaluated by TotalsHavingTransform (WITH TOTALS), covering the onCancel -> cancelExecution
# path for a long-running guest function — unlike the failpoint-based HAVING tests
# (04658/04661/04671), which pause only after `expression->execute` has returned.
# Uses infinite_loop_signal (which calls _wasm_signal_ready before entering its infinite loop)
# from faulty.wasm. The host function _wasm_signal_ready fires the wasm_guest_pause failpoint
# to prove that guest code actually started executing inside the HAVING expression.
# no-parallel: wasm_guest_pause is a global PAUSEABLE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_having_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_having_wasm_${CLICKHOUSE_DATABASE}.out"

# EXIT trap covers failed reruns that crashed before explicit cleanup.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04813" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_04813'\''" 2>/dev/null' EXIT

# Use module/function names unique to 04813 for isolation from other WASM tests.
# Drop function before deleting module — DELETE throws CANNOT_DROP_FUNCTION
# while a function backed by the module still exists.
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04813"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04813'"

# Load the WASM module with the infinite_loop_signal function
cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_04813', code FROM input('code String') FORMAT RawBlob"

# Create the infinite_loop_04813 function using infinite_loop_signal from faulty.wasm
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE OR REPLACE FUNCTION infinite_loop_04813 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_04813' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

# The HAVING expression of a WITH TOTALS query must be evaluated by TotalsHavingTransform —
# that is the code path under test.
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    EXPLAIN PIPELINE
    SELECT number % 2 AS k, count()
    FROM numbers(10)
    GROUP BY k WITH TOTALS
    HAVING infinite_loop_04813(toUInt32(count())) = 1
" | grep -qF "TotalsHaving" || { echo "FAIL: no TotalsHaving in the pipeline"; exit 1; }

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"

# Start a query that uses infinite_loop_signal in HAVING of a WITH TOTALS query.
# The function calls _wasm_signal_ready() before entering its infinite loop,
# and the host function fires the wasm_guest_pause failpoint — proving that
# guest code actually started executing inside `expression->execute` of
# TotalsHavingTransform::transform.
${CLICKHOUSE_CLIENT} --query_id="$query_id" --allow_experimental_analyzer=1 --query "
    SELECT number % 2 AS k, count()
    FROM numbers(10)
    GROUP BY k WITH TOTALS
    HAVING infinite_loop_04813(toUInt32(count())) = 1
    FORMAT Null
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

# Wait for the failpoint to be hit — proves the WASM guest code is actually executing.
# The wait has no built-in timeout, so bound it: if the guest never reaches
# _wasm_signal_ready (a regression before guest execution starts), fail explicitly
# instead of hanging the whole check. Kill the stuck query (async — a SYNC kill of an
# unkillable query would hang again) and exit without waiting for the background job.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT wasm_guest_pause PAUSE"
then
    echo "FAIL: timed out waiting for the wasm_guest_pause failpoint — the WASM guest code did not start executing"
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

# Kill the query (ASYNC) — this triggers TotalsHavingTransform::onCancel -> cancelExecution ->
# interrupt_source.request_stop(). The StopCallback registered in invokeImpl sets WasmEdge's
# cost limit to 0, causing CostLimitExceeded on the next instruction after the host function returns.
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint — unblocks _wasm_signal_ready(), which returns to the WASM guest code
# The guest then enters the infinite loop, and the first WASM instruction triggers
# CostLimitExceeded (since the cost limit was set to 0 by the KILL callback).
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

wait

# Assert cancellation was detected, not timeout. The interruption of the in-flight guest can
# surface to the client in two ways, depending on which side wins the race after the kill:
# QUERY_WAS_CANCELLED when the cancellation check in `ExpressionActions::execute` (or the pulling
# executor) notices the kill first, or WASM_ERROR when the interrupted guest's own error
# ("WASM execution was stopped by request" under wasmtime, a cost-limit trap under WasmEdge) is
# rethrown by `ISimpleTransform::work` first. Both prove the KILL interrupted the running guest:
# the guest loop is infinite, so without the interruption the query would never terminate and the
# test would hang in `wait`.
grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: query was not cancelled"; exit 1; }

# Clean up
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04813"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04813'"

echo "OK"
