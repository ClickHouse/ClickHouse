#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan

# Verify that cancellation between dispatch and the totals-port expression evaluation skips the
# expression. The post-expression failpoint stays enabled: without the guard, the query reaches
# it after the kill and times out instead of reporting `QUERY_WAS_CANCELLED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_totals_port_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/${query_id}.out"

trap '
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause" 2>/dev/null
' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_totals_before_expression_pause"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_totals_pause"

# `TabSeparated` consumes the totals port. Each regular group passes its `HAVING` expression,
# while the totals row reaches the dedicated `prepareTotals` execution site.
# The client must outlive the whole orchestration below (waiting for the failpoint, killing the
# query and waiting for the cancellation to be observed), otherwise `timeout` terminates it before
# the server reports `QUERY_WAS_CANCELLED` and the test fails spuriously on slow builds.
timeout 120 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT number % 10 AS k, count()
    FROM numbers(1000000)
    GROUP BY k WITH TOTALS
    HAVING sipHash64(count()) % 2 >= 0
    FORMAT TabSeparated
    SETTINGS max_threads = 1
" >"$output_file" 2>&1 &
client_pid=$!

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT totals_having_transform_totals_before_expression_pause PAUSE"
then
    echo "FAIL: timed out waiting for totals_having_transform_totals_before_expression_pause"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
    exit 1
fi

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null

# Do not release the failpoint until the asynchronous kill has reached the query. Otherwise,
# the query can resume and park at the post-expression failpoint before cancellation is set.
# Poll over HTTP with a wall-clock deadline: starting `clickhouse-client` hundreds of times costs
# more wall-clock time than the query client is allowed to live on a debug or sanitizer build.
cancelled=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline ))
do
    cancelled=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
    [[ "$cancelled" -ge 1 ]] && break
    sleep 0.1
done
[[ "$cancelled" -ge 1 ]] || { echo "FAIL: the query was not marked as cancelled in system.processes"; exit 1; }

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_before_expression_pause"

wait "$client_pid"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause"

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: the client did not report QUERY_WAS_CANCELLED"; cat "$output_file"; exit 1; }

echo "OK"
