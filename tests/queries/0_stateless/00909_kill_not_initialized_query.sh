#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS cannot_kill_query"
$CLICKHOUSE_CLIENT -q "CREATE TABLE cannot_kill_query (x UInt64) ENGINE = MergeTree ORDER BY x" &> /dev/null
$CLICKHOUSE_CLIENT -q "INSERT INTO cannot_kill_query SELECT * FROM numbers(10000000)" &> /dev/null

# This SELECT query will run for a long time. It's used as bloker for ALTER query. It will be killed with SYNC kill.
query_for_pending="SELECT count() FROM cannot_kill_query WHERE NOT ignore(sleep(1)) SETTINGS max_threads = 1, max_block_size = 1"
$CLICKHOUSE_CLIENT -q "$query_for_pending" &>/dev/null &

sleep 1 # queries should be in strict order

# This ALTER query will wait until $query_for_pending finished. Also it will block $query_to_kill.
$CLICKHOUSE_CLIENT -q "ALTER TABLE cannot_kill_query MODIFY COLUMN x UInt64" &>/dev/null &

sleep 1

# This SELECT query will also run for a long time. Also it's blocked by ALTER query. It will be killed with ASYNC kill.
# This is main idea which we check -- blocked queries can be killed with ASYNC kill.
query_to_kill="SELECT sum(1) FROM cannot_kill_query WHERE NOT ignore(sleep(1)) SETTINGS max_threads = 1"
$CLICKHOUSE_CLIENT -q "$query_to_kill" &>/dev/null &

sleep 1 # just to be sure that kill of $query_to_kill will be executed after $query_to_kill.

# Kill $query_to_kill with ASYNC kill. We will check that information about KILL is not lost.
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query='$query_to_kill' ASYNC" &>/dev/null

sleep 1

# Kill $query_for_pending SYNC. This query is not blocker, so it should be killed fast.
clickhouse_client_timeout 20 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query='$query_for_pending' SYNC" &>/dev/null

# Both queries have to be killed, doesn't matter with SYNC or ASYNC kill
for _ in {1..15}
do
    sleep 1
    no_first_query=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes where query='$query_for_pending'")
    no_second_query=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes where query='$query_to_kill'")
    if [ "$no_first_query" == "0" ] && [ "$no_second_query" == "0" ]; then
        echo "killed"
        break
    fi
done

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS cannot_kill_query" &>/dev/null
