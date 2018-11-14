#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

QUERY_FIELND_NUM=4

$CLICKHOUSE_CLIENT --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 4" &>/dev/null &
sleep 1
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query LIKE 'SELECT sleep(%' AND (elapsed >= 0.) SYNC" | cut -f $QUERY_FIELND_NUM

$CLICKHOUSE_CLIENT --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 5" &>/dev/null &
sleep 1
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query = 'SELECT sleep(1) FROM system.numbers LIMIT 5' ASYNC" | cut -f $QUERY_FIELND_NUM

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 ASYNC"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 SYNC FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 1 TEST" &>/dev/null


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.cannot_kill_query"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.cannot_kill_query (x UInt64) ENGINE = MergeTree ORDER BY x" &> /dev/null
$CLICKHOUSE_CLIENT -q "INSERT INTO test.cannot_kill_query SELECT * FROM numbers(10000000)" &> /dev/null

query_for_pending="SELECT count() FROM test.cannot_kill_query WHERE NOT ignore(sleep(1)) SETTINGS max_threads=1"
$CLICKHOUSE_CLIENT -q "$query_for_pending" &>/dev/null &

$CLICKHOUSE_CLIENT -q "ALTER TABLE test.cannot_kill_query MODIFY COLUMN x UInt64" &>/dev/null &

query_to_kill="SELECT sum(1) FROM test.cannot_kill_query WHERE NOT ignore(sleep(1)) SETTINGS max_threads=1"
$CLICKHOUSE_CLIENT -q "$query_to_kill" &>/dev/null &

sleep 1 # just to be sure that 'KILL ...' will be executed after 'SELECT ... WHERE NOT ignore(sleep(1))'

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query='$query_to_kill'" &>/dev/null &

# 'SELECT ... WHERE NOT ignore(sleep(1))' is executing much longer than 3 secs, so this sleep doesn't fail test logic
# but guarantees to eliminate flaps, when SELECT from system.process is executed before KILL is completed
sleep 3

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes where query='$query_to_kill'"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query='$query_for_pending'" &>/dev/null & # kill pending query

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.cannot_kill_query" &>/dev/null
