#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for LIMIT BY queries.
# Tests both transformCommon and transformInOrder paths.

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

# Test transformCommon path
test_limit_by_cancellation "common" "
    SELECT
        number % 100000000 AS category,
        number AS value
    FROM numbers(100000000)
    LIMIT 1 BY category
    FORMAT Null
    SETTINGS max_block_size=100000000, max_threads=1, max_rows_to_read=0
" || exit 1

# Test transformInOrder path
test_limit_by_cancellation "inorder" "
    SELECT
        concat(toString(number), repeat('x', 50)) AS cat1,
        concat(toString(number), repeat('y', 50)) AS cat2,
        number AS value
    FROM numbers(100000000)
    LIMIT 1 BY
        cat1,
        cat2
    FORMAT Null
    SETTINGS max_block_size = 100000000, max_threads = 1, max_rows_to_read=0
" || exit 1
