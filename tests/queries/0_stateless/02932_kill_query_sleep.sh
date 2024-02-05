#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function wait_query_started()
{
    local query_id="$1"
    timeout=60
    start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.processes WHERE query_id='$query_id' SETTINGS use_query_cache=0") == 0 ]]; do
          if ((EPOCHSECONDS-start > timeout )); then
             echo "Timeout while waiting for query $query_id to start"
             exit 1
          fi
          sleep 0.1
    done
}


function kill_query()
{
    local query_id="$1"
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id='$query_id'" >/dev/null
    timeout=60
    start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.processes WHERE query_id='$query_id' SETTINGS use_query_cache=0") != 0 ]]; do
          if ((EPOCHSECONDS-start > timeout )); then
             echo "Timeout while waiting for query $query_id to cancel"
             exit 1
          fi
          sleep 0.1
    done
}


sleep_query_id="sleep_query_id_02932_kill_query_sleep_${CLICKHOUSE_DATABASE}_$RANDOM"

# This sleep query wants to sleep for 1000 seconds (which is too long).
# We're going to cancel this query later.
sleep_query="SELECT sleep(1000)"

$CLICKHOUSE_CLIENT --query_id="$sleep_query_id" --function_sleep_max_microseconds_per_block="1000000000" --query "$sleep_query" >/dev/null 2>&1 &
wait_query_started "$sleep_query_id"

echo "Cancelling query"
kill_query "$sleep_query_id"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS;"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id='$sleep_query_id' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"
