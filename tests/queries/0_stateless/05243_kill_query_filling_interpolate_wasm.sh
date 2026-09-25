#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY interrupts a WASM UDF running in an `ORDER BY ... WITH FILL INTERPOLATE` expression.
# This covers FillingTransform::onCancel -> cancelExecution while the guest is executing.
# no-parallel: wasm_guest_pause is a global PAUSEABLE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_filling_interpolate_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_filling_interpolate_wasm_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05243" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05243'\''" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05243"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05243'"

cat "${CUR_DIR}"/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05243', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05243 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05243' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"

# The first filling row evaluates the INTERPOLATE expression from the previous row, where `x` is non-zero,
# so the guest enters its infinite loop (`webassembly_udf_max_fuel = 0` disables the fuel limit).
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT n, x FROM (SELECT number AS n, toUInt32(number + 1) AS x FROM numbers(1))
    ORDER BY n WITH FILL TO 5 INTERPOLATE (x AS infinite_loop_05243(x))
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT wasm_guest_pause PAUSE"
then
    echo "FAIL: timed out waiting for the wasm_guest_pause failpoint — the INTERPOLATE expression did not start executing"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
    exit 1
fi

# onCancel forwards cancellation into the function currently running in FillingTransform.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

wait

grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION infinite_loop_05243"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05243'"

echo "OK"
