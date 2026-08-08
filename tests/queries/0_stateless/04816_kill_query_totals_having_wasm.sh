#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY interrupts a WASM UDF that is still running inside the totals-row HAVING
# expression evaluated by TotalsHavingTransform::prepareTotals, covering the
# onCancel -> cancelExecution path for the totals-port execution site — unlike
# 04813_kill_query_having_wasm, which proves it only for TotalsHavingTransform::transform
# (FORMAT Null never consumes the totals port), and unlike 04661/04671, whose failpoints
# pause only after `expression->execute` has already returned.
# Uses infinite_loop_signal (which calls _wasm_signal_ready before entering its infinite loop)
# from faulty.wasm: it returns immediately for argument 0 and loops forever otherwise. The
# HAVING argument is toUInt32(count() - 1), and every group has exactly one row, so all regular
# rows evaluate with argument 0 and pass through `transform` instantly; only the totals row
# (count() = 8, argument 7) enters the guest loop — and the totals row is evaluated only in
# `prepareTotals`. The host function _wasm_signal_ready fires the wasm_guest_pause failpoint
# to prove that guest code is executing there when KILL QUERY arrives.
# no-parallel: wasm_guest_pause is a global PAUSEABLE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_totals_having_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_totals_having_wasm_${CLICKHOUSE_DATABASE}.out"

# EXIT trap covers failed reruns that crashed before explicit cleanup.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04816" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_04816'\''" 2>/dev/null' EXIT

# Use module/function names unique to 04816 for isolation from other WASM tests.
# Drop function before deleting module — DELETE throws CANNOT_DROP_FUNCTION
# while a function backed by the module still exists.
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04816"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04816'"

# Load the WASM module with the infinite_loop_signal function
cat ${CUR_DIR}/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_04816', code FROM input('code String') FORMAT RawBlob"

# Create the infinite_loop_04816 function using infinite_loop_signal from faulty.wasm
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE OR REPLACE FUNCTION infinite_loop_04816 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_04816' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

# The HAVING expression of a WITH TOTALS query must be evaluated by TotalsHavingTransform —
# that is the code path under test.
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    EXPLAIN PIPELINE
    SELECT number AS k, count()
    FROM numbers(8)
    GROUP BY k WITH TOTALS
    HAVING infinite_loop_04816(toUInt32(count() - 1)) = 0
" | grep -o "TotalsHaving" | head -1

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"

# Start the query. GROUP BY number over numbers(8) makes every group a single row, so in
# TotalsHavingTransform::transform the UDF argument is count() - 1 = 0 for every row and the
# guest returns immediately without signalling. TabSeparated (unlike FORMAT Null) consumes the
# totals port, so after the input is exhausted `prepareTotals` evaluates the HAVING expression
# for the totals row, where count() = 8 — the only call with a non-zero argument, which fires
# _wasm_signal_ready and then loops forever inside `expression->execute` of `prepareTotals`.
${CLICKHOUSE_CLIENT} --query_id="$query_id" --allow_experimental_analyzer=1 --query "
    SELECT number AS k, count()
    FROM numbers(8)
    GROUP BY k WITH TOTALS
    HAVING infinite_loop_04816(toUInt32(count() - 1)) = 0
    FORMAT TabSeparated
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

# Wait for the failpoint to be hit — proves the WASM guest code is actually executing inside
# the totals-row evaluation (no other call site passes a non-zero argument).
# The wait has no built-in timeout, so bound it: if the guest never reaches
# _wasm_signal_ready (a regression before guest execution starts), fail explicitly
# instead of hanging the whole check. Kill the stuck query (async — a SYNC kill of an
# unkillable query would hang again) and exit without waiting for the background job.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT wasm_guest_pause PAUSE"
then
    echo "FAIL: timed out waiting for the wasm_guest_pause failpoint — the WASM guest code did not start executing in prepareTotals"
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

# Kill the query (ASYNC) — this triggers TotalsHavingTransform::onCancel -> cancelExecution ->
# interrupt_source.request_stop(). The StopCallback registered in invokeImpl sets the engine's
# cost limit to 0, causing CostLimitExceeded on the next instruction after the host function returns.
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint — unblocks _wasm_signal_ready(), which returns to the WASM guest code
# The guest then enters the infinite loop, and the first WASM instruction triggers
# CostLimitExceeded (since the cost limit was set to 0 by the KILL callback).
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

wait

# Assert cancellation was detected, not timeout. The interruption of the in-flight guest can
# surface to the client in two ways, depending on which side wins the race after the kill:
# QUERY_WAS_CANCELLED when the pulling executor notices the kill first, or WASM_ERROR when the
# interrupted guest's own error ("WASM execution was stopped by request" under wasmtime,
# a cost-limit trap under WasmEdge) is rethrown first — in `prepareTotals` the main stream is
# already drained, so the latter is the usual outcome. Both prove the KILL interrupted the
# running guest: the guest loop is infinite, so without the interruption the query would never
# terminate and the test would hang in `wait`.
grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: query was not cancelled"; exit 1; }

# Clean up
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_04816"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_04816'"

echo "OK"
