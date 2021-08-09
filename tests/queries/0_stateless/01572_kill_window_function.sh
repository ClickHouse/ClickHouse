#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$1' and current_database = currentDatabase()") == 0 ]]; do sleep 0.1; done
}

query_id="01572_kill_window_function"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "SELECT count(1048575) OVER (PARTITION BY intDiv(NULL, number) ORDER BY number DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND 1048575 FOLLOWING) FROM numbers(255, 1048575)" >/dev/null 2>&1 &
client_pid=$!
wait_for_query_to_start "$query_id"
$CLICKHOUSE_CLIENT --query "kill query where query_id = '$query_id' and current_database = currentDatabase() format Null"
wait $client_pid || echo $?

# We have tested for Ctrl+C.
# The following client flags don't cancel, but should: --max_execution_time,
# --receive_timeout. Probably needs asynchonous calculation of query limits, as
# discussed with Nikolay on TG: https://t.me/c/1214350934/21492

