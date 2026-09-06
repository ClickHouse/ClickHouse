#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for queries with deep nested expression functions in filter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_expression_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_expression_${CLICKHOUSE_DATABASE}.out"

# Deep nested sipHash64() functions - requires expression evaluation to be cancelled properly.
# The client is timeout-bounded: if the cancellation is not observed inside the expression
# evaluation, the query grinds through all 100 million rows and the `wait` below would hold the
# whole check; the test has to fail locally instead. The bound is generous compared to the other
# kill-query tests because here a whole 10-million-row block of nested `sipHash64` calls can be in
# flight when the kill arrives, and the cancellation is only observed between two actions.
timeout 300 $CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    SELECT count()
    FROM numbers(100000000)
    WHERE sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(number))))))))))))))))))))) % 2 =1
    FORMAT Null
    SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0
" >"$output_file" 2>&1 &

wait_for_query_to_start "$query_id"

# Use an asynchronous KILL and explicitly disable waiting for query completion: the test harness
# can randomize `http_wait_end_of_query`, which would otherwise make this control request wait for
# the deliberately long-running target query.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null

wait

# Assert cancellation was detected, not normal completion (or a client killed by its `timeout`)
grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
