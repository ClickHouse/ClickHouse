#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for LIMIT BY queries.
# Tests both transformCommon (no ORDER BY) and transformInOrder
# (ORDER BY with LIMIT BY columns as prefix) paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_limit_by_cancellation()
{
    local description="$1"
    local sql_query="$2"

    local query_id="limit_by_cancel_${CLICKHOUSE_DATABASE}_${description}_$RANDOM"

    $CLICKHOUSE_CLIENT --query_id="$query_id" --query "$sql_query" >/dev/null 2>&1 &
    local CLIENT_PID=$!

    wait_for_query_to_start "$query_id"

    # Use async KILL (without SYNC) to avoid blocking if propagation is slow.
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

    wait $CLIENT_PID

    echo "OK"
}

# Test transformCommon path: no ORDER BY, uses hashmap-based group tracking
test_limit_by_cancellation "common" "
    SELECT
        number % 100000000 AS category,
        number AS value
    FROM numbers(100000000)
    LIMIT 1 BY category
    FORMAT Null
    SETTINGS max_block_size=100000000, max_threads=1
" || exit 1

# Test transformInOrder path: ORDER BY with LIMIT BY columns as prefix, uses sorted group tracking
test_limit_by_cancellation "inorder" "
    SELECT
        concat(toString(number), repeat('aaaaaaaaaa', 1000)) AS cat1,
        concat(toString(number), repeat('bbbbbbbbbb', 1000)) AS cat2,
        number AS value
    FROM numbers(1200000)
    ORDER BY cat1, cat2, value
    LIMIT 1 BY cat1, cat2
    FORMAT Null
    SETTINGS max_block_size=1200000, max_threads=1
" || exit 1
