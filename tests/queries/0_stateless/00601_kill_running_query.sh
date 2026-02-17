#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

function wait_for_query_to_start()
{
    local timeout=30
    local start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout waiting for query $1 to start" >&2
            exit 1
        fi
        sleep 0.1
    done
}

# Use sleep-based query that is guaranteed to run long enough to be killed,
# unlike the previous groupArray query that could complete too quickly on fast machines.
${CLICKHOUSE_CURL_COMMAND} -q --max-time 30 -sS "$CLICKHOUSE_URL&query_id=test_00601_$CLICKHOUSE_DATABASE" -d 'SELECT sleep(1) FROM numbers(10000) SETTINGS max_block_size = 1, max_rows_to_read = 0' > /dev/null &
wait_for_query_to_start "test_00601_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = 'test_00601_$CLICKHOUSE_DATABASE'"
wait || true
