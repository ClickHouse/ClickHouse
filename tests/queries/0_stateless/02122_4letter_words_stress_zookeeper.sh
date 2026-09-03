#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-catalog
# Tag no-shared-catalog -- does not support tables with the same ZK path, especially with the same name

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


function four_letter_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    declare -a FOUR_LETTER_COMMANDS=("conf" "cons" "crst" "envi" "ruok" "srst" "srvr" "stat" "wchc" "wchs" "dirs" "mntr" "isro")
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        command=${FOUR_LETTER_COMMANDS[$RANDOM % ${#FOUR_LETTER_COMMANDS[@]} ]}
        $CLICKHOUSE_KEEPER_CLIENT -q "$command" 1>/dev/null
    done

}

function create_drop_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        num=$(($RANDOM % 10 + 1))
        $CLICKHOUSE_CLIENT --query "CREATE TABLE test_table$num (key UInt64, value1 UInt8, value2 UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_table$num', '0') ORDER BY key"
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table$num"
    done
}

TIMEOUT=15

four_letter_thread &
four_letter_thread &
four_letter_thread &
four_letter_thread &

create_drop_thread 2> /dev/null &
create_drop_thread 2> /dev/null &
create_drop_thread 2> /dev/null &
create_drop_thread 2> /dev/null &


wait

for num in $(seq 1 10); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table$num" 2>/dev/null
    while  [ $? -ne 0 ]; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_table$num" 2>/dev/null
    done
done

# still alive
$CLICKHOUSE_CLIENT --query "SELECT 1"
