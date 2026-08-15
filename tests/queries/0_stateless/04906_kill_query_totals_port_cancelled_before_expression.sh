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
timeout 30 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT number % 10 AS k, count()
    FROM numbers(1000000)
    GROUP BY k WITH TOTALS
    HAVING sipHash64(count()) % 2 >= 0
    FORMAT TabSeparated
    SETTINGS max_threads = 1
" >"$output_file" 2>&1 &
client_pid=$!

if ! timeout 30 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT totals_having_transform_totals_before_expression_pause PAUSE"
then
    echo "FAIL: timed out waiting for totals_having_transform_totals_before_expression_pause"
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
    exit 1
fi

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_before_expression_pause"

wait "$client_pid"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause"

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
