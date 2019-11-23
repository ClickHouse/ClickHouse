#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS a;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS b;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE a (x UInt64) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE b (x UInt64) ENGINE = Memory;"

function thread1()
{
	while true; do
                seq 1 11000000 | $CLICKHOUSE_CLIENT --query_id=11 --query="INSERT INTO a(x) FORMAT TSV"
		sleep 1
		$CLICKHOUSE_CLIENT --query="kill query where query_id='22'" SYNC 

        done
}

function thread2()
{
        while true; do
                seq 1 11000000 | $CLICKHOUSE_CLIENT --query_id=22 --query="INSERT INTO b(x) FORMAT TSV" 
		sleep 1
		$CLICKHOUSE_CLIENT --query="kill query where query_id='11'" SYNC
        done
}


# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

TIMEOUT=20

timeout $TIMEOUT bash -c thread1 2>&1 > /dev/null &
timeout $TIMEOUT bash -c thread2 2>&1 > /dev/null &

wait

echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE a"
$CLICKHOUSE_CLIENT --query "DROP TABLE b"


