#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY interrupts a WASM UDF running in a `CHECK` constraint of the target table.
# `CheckConstraintsTransform` evaluates the constraint expressions of an `INSERT`, so this covers
# `CheckConstraintsTransform::onCancel` -> `cancelExecution` while the guest is executing.
# no-parallel: wasm_guest_pause is a global PAUSEABLE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_check_constraints_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_check_constraints_wasm_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP TABLE IF EXISTS kill_query_check_constraints_wasm" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_05047" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05047'\''" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP TABLE IF EXISTS kill_query_check_constraints_wasm"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION IF EXISTS infinite_loop_05047"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05047'"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05047', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/faulty.wasm

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05047 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05047' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "
    CREATE TABLE kill_query_check_constraints_wasm
    (
        x UInt32,
        CONSTRAINT c CHECK infinite_loop_05047(x) = 0
    ) ENGINE = Memory
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"

timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --allow_experimental_analyzer=1 --query "
    INSERT INTO kill_query_check_constraints_wasm
    SELECT toUInt32(1)
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT wasm_guest_pause PAUSE"
then
    echo "FAIL: timed out waiting for the wasm_guest_pause failpoint — the constraint expression did not start executing"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
    exit 1
fi

# onCancel forwards cancellation into the function currently running in CheckConstraintsTransform.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

wait

# `cancelExecution` only requests a stop on the shared interrupt source, so the guest can throw
# `WASM_ERROR` before `ExpressionActions::execute` reaches its cancellation check.
grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP TABLE kill_query_check_constraints_wasm"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q "DROP FUNCTION infinite_loop_05047"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05047'"

echo "OK"
