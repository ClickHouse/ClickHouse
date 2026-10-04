#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Verify that cancellation between task dispatch and the evaluation of an omitted-field `DEFAULT`
# expression skips that evaluation. `adding_defaults_transform_pause` stays enabled: without the
# pre-execution cancellation guard in `AddingDefaultsTransform`, the query reaches it after the
# kill and times out instead of reporting `QUERY_WAS_CANCELLED`.
# no-parallel: the failpoints are global, an unrelated query could consume them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_before_expression" 2>/dev/null
' EXIT

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_before_expression"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE kill_query_adding_defaults_before_expression (x UInt64, y UInt64 DEFAULT sipHash64(x)) ENGINE = Memory"

query_id="kill_query_adding_defaults_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/${query_id}.out"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT adding_defaults_transform_before_expression_pause"

# The column `y` is omitted from the data, so `AddingDefaultsTransform` evaluates its `DEFAULT`.
# The `INSERT` goes over HTTP so that the data is parsed on the server in the main insert pipeline
# (`getSourceFromASTInsertQuery`): `clickhouse-client` parses inline data on the client side, and
# `async_insert = 1` (the default) would move the evaluation to a detached flush thread of
# `AsynchronousInsertQueue` that `KILL QUERY` cannot reach.
timeout 120 ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&async_insert=0&max_threads=1&input_format_defaults_for_omitted_fields=1" \
    --data-binary 'INSERT INTO kill_query_adding_defaults_before_expression FORMAT JSONEachRow {"x":1}' >"$output_file" 2>&1 &
curl_pid=$!

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT adding_defaults_transform_before_expression_pause PAUSE"
then
    echo "FAIL: timed out waiting for adding_defaults_transform_before_expression_pause"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_expression_pause"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
    exit 1
fi

# The query is deliberately held at the failpoint, so a synchronous `KILL QUERY` waits for it to
# finish and prevents this test from releasing the failpoint. The stateless-test random settings
# can enable `http_wait_end_of_query`, which makes the HTTP request wait for the killed query even
# with `ASYNC`; override it for the control request.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null

# Do not release the failpoint until the asynchronous kill has reached the query. Otherwise the
# query can resume and park at the post-expression failpoint before cancellation is set.
cancelled=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline ))
do
    cancelled=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
    [[ "$cancelled" -ge 1 ]] && break
    sleep 0.1
done
[[ "$cancelled" -ge 1 ]] || { echo "FAIL: the query was not marked as cancelled in system.processes"; exit 1; }

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT adding_defaults_transform_pause"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_before_expression_pause"

wait "$curl_pid"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT adding_defaults_transform_pause"

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: the query did not report QUERY_WAS_CANCELLED"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_adding_defaults_before_expression"

echo "OK"
