#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that `KILL QUERY` reaches an omitted-field `DEFAULT` expression even when the cancellation
# lands after `AddingDefaultsTransform` published the actions for `onCancel` but before the
# evaluation started. `AddingDefaultsTransform` publishes `current_actions` before it can observe
# the cancellation, so `onCancel` always forwards `cancelExecution` into the function that is
# about to run: without that ordering `onCancel` sees no actions and the WASM guest loops forever.
# no-parallel: the failpoints are global, an unrelated query could consume them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_adding_defaults_after_publication_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/${query_id}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_execute_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_after_publication" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05056" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05056'\''" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_execute_pause"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_after_publication"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05056"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05056'"

cat "${CUR_DIR}"/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05056', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05056 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05056' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE kill_query_adding_defaults_after_publication
    (
        x UInt32,
        y UInt32 DEFAULT infinite_loop_05056(x)
    ) ENGINE = Memory
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT adding_defaults_transform_before_execute_pause"

# The column `y` is omitted from the data, so `AddingDefaultsTransform` evaluates its `DEFAULT`.
# The `INSERT` goes over HTTP so that the data is parsed on the server in the main insert pipeline
# (`getSourceFromASTInsertQuery`): `clickhouse-client` parses inline data on the client side, and
# `async_insert = 1` (the default) would move the evaluation to a detached flush thread of
# `AsynchronousInsertQueue` that `KILL QUERY` cannot reach.
timeout 120 ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&async_insert=0&webassembly_udf_max_fuel=0&max_threads=1&input_format_defaults_for_omitted_fields=1" \
    --data-binary 'INSERT INTO kill_query_adding_defaults_after_publication FORMAT JSONEachRow {"x":1}' >"$output_file" 2>&1 &
curl_pid=$!

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT adding_defaults_transform_before_execute_pause PAUSE"
then
    echo "FAIL: timed out waiting for adding_defaults_transform_before_execute_pause"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_execute_pause"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null
    exit 1
fi

# The query is deliberately held at the failpoint, so a synchronous `KILL QUERY` waits for it to
# finish and prevents this test from releasing the failpoint. The stateless-test random settings
# can enable `http_wait_end_of_query`, which makes the HTTP request wait for the killed query even
# with `ASYNC`; override it for the control request.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null

# Do not release the failpoint until the cancellation has reached the query: the point of the test
# is that the evaluation starts after the query is already cancelled.
cancelled=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline ))
do
    cancelled=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
    [[ "$cancelled" -ge 1 ]] && break
    sleep 0.1
done
[[ "$cancelled" -ge 1 ]] || { echo "FAIL: the query was not marked as cancelled in system.processes"; exit 1; }

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_execute_pause"

wait "$curl_pid"

grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: the query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_adding_defaults_after_publication"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION infinite_loop_05056"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05056'"

echo "OK"
