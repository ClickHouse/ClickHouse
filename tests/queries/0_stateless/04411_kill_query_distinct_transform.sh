#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Start a long-running DISTINCT query in the background.
$CLICKHOUSE_CLIENT \
    --query_id="$query_id" \
    --max_block_size=50000000 \
    --query "SELECT DISTINCT number % 10000000 FROM numbers(50000000) FORMAT Null" \
    > /dev/null 2>&1 &

wait_for_query_to_start "$query_id"

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id='$query_id'" > /dev/null

wait

echo "OK"
