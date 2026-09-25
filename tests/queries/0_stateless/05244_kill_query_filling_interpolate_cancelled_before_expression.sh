#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that a KILL QUERY landing in FillingTransform right before an `INTERPOLATE` expression is evaluated
# skips the expression instead of starting it. The query is held at `filling_transform_before_interpolate_pause`,
# killed, and only then `wasm_guest_pause` is armed and the query is released: if the expression were still
# evaluated, the WASM guest would park at `wasm_guest_pause` and the query would time out.
# It also covers the filling rows generated after the cancellation: `ExpressionActions::execute` returns
# empty result columns when it stops early, and these must not be inserted into the result.
# no-parallel: wasm_guest_pause and filling_transform_before_interpolate_pause are global failpoints.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_filling_interpolate_before_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_filling_interpolate_before_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filling_transform_before_interpolate_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05244" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05244'\''" 2>/dev/null' EXIT

scalar()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

kill_query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$1' ASYNC" >/dev/null
}

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filling_transform_before_interpolate_pause"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05244"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05244'"

cat "${CUR_DIR}"/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05244', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05244 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05244' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filling_transform_before_interpolate_pause"

timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT n, x FROM (SELECT number AS n, toUInt32(number + 1) AS x FROM numbers(1))
    ORDER BY n WITH FILL TO 5 INTERPOLATE (x AS infinite_loop_05244(x))
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filling_transform_before_interpolate_pause PAUSE"
then
    echo "FAIL: timed out waiting for the filling_transform_before_interpolate_pause failpoint"
    kill_query "$query_id"
    exit 1
fi

kill_query "$query_id"

# Do not release the failpoint until the asynchronous kill has reached the query.
cancelled=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline ))
do
    cancelled=$(scalar "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
    [[ "$cancelled" -ge 1 ]] && break
    sleep 0.1
done
if [[ "$cancelled" -lt 1 ]]
then
    echo "FAIL: the query was not marked as cancelled in system.processes"
    exit 1
fi

# Armed only after the kill is confirmed: a query that still started the INTERPOLATE expression parks here.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filling_transform_before_interpolate_pause"

# Wait for the query client with a bound. `timeout 60` above guarantees it ends eventually.
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )) && jobs -r | grep -q .
do
    sleep 0.1
done
if jobs -r | grep -q .
then
    echo "FAIL: the INTERPOLATE expression was started after the query was cancelled"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"
    wait
    exit 1
fi
wait

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

grep -q "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION infinite_loop_05244"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05244'"

echo "OK"
