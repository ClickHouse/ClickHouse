#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e


function thread1()
{
    while true; do 
    	$CLICKHOUSE_CLIENT --query "SELECT printLog('thread1', toUInt64(10)) FORMAT Null;"
    done
}

function thread2()
{
    while true; do 
    	$CLICKHOUSE_CLIENT --query "SELECT printLog('thread2', toUInt64(10)) FORMAT Null;"
    done
}

function thread3()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "SELECT printLog('thread3', toUInt64(10)) FORMAT NULL;"
    done
}

function thread4()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "SELECT printLog('thread4', toUInt64(10)) FORMAT NULL;"
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait

