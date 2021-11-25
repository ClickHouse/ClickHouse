#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


function four_letter_thread()
{
    declare -a FOUR_LETTER_COMMANDS=("conf" "cons" "crst" "envi" "ruok" "srst" "srvr" "stat" "wchc" "wchs" "dirs" "mntr" "isro")
    while true; do
        command=${FOUR_LETTER_COMMANDS[$RANDOM % ${#FOUR_LETTER_COMMANDS[@]} ]}
        echo $command | nc ${CLICKHOUSE_HOST} ${CLICKHOUSE_PORT_KEEPER} 1>/dev/null
    done

}

function create_drop_thread()
{
    while true; do
        num=$RANDOM
        $CLICKHOUSE_CLIENT --query "CREATE TABLE test_table$num (key UInt64, value1 UInt8, value2 UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_table$num', '0') ORDER BY key"
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table$num"
    done
}

export -f four_letter_thread;
export -f create_drop_thread;

TIMEOUT=15

timeout $TIMEOUT bash -c four_letter_thread 2> /dev/null &
timeout $TIMEOUT bash -c four_letter_thread 2> /dev/null &
timeout $TIMEOUT bash -c four_letter_thread 2> /dev/null &
timeout $TIMEOUT bash -c four_letter_thread 2> /dev/null &

timeout $TIMEOUT bash -c create_drop_thread 2> /dev/null &
timeout $TIMEOUT bash -c create_drop_thread 2> /dev/null &
timeout $TIMEOUT bash -c create_drop_thread 2> /dev/null &
timeout $TIMEOUT bash -c create_drop_thread 2> /dev/null &


wait

# still alive
$CLICKHOUSE_CLIENT --query "SELECT 1"
