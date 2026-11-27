#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for queries with deep nested expression functions.
# Ref: https://github.com/ClickHouse/ClickHouse/issues/97844

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_expression_${CLICKHOUSE_DATABASE}_$RANDOM"

# Deep nested sipHash64() functions - requires expression evaluation to be cancelled properly
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    SELECT sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(number)))))))))))))))))))))
    FROM numbers(100000000)
    FORMAT Null
    SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0
" >/dev/null 2>&1 &

wait_for_query_to_start "$query_id"

# Use async KILL (without SYNC) to avoid blocking if propagation is slow.
# Kill the query while it's processing expressions
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

wait

echo "OK"
