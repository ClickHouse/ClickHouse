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


# Test cancellation of scalar subquery
scalar_query_id="scalar_query_id_04003_${CLICKHOUSE_DATABASE}_$RANDOM"
scalar_query="SELECT (SELECT max(number) FROM system.numbers) + 1"

$CLICKHOUSE_CLIENT --query_id="$scalar_query_id" --query="$scalar_query" >/dev/null 2>&1 &
wait_query_started "$scalar_query_id"

echo "Cancelling scalar subquery"
kill_query "$scalar_query_id"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id='$scalar_query_id' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"

# Test cancellation of IN subquery with a MergeTree table
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t (col UInt64) ENGINE = MergeTree() ORDER BY col"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_TEST_UNIQUE_NAME}_t VALUES (rand()), (rand()), (rand())"

in_query_id="in_query_id_04003_${CLICKHOUSE_DATABASE}_$RANDOM"
in_query="SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t WHERE col IN (SELECT max(rand()) FROM system.numbers)"

$CLICKHOUSE_CLIENT --query_id="$in_query_id" --query="$in_query" >/dev/null 2>&1 &
wait_query_started "$in_query_id"

echo "Cancelling IN subquery"
kill_query "$in_query_id"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id='$in_query_id' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"

$CLICKHOUSE_CLIENT --query "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
