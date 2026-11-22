#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS data_a_01019;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS data_b_01019;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE data_a_01019 (x UInt64) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE data_b_01019 (x UInt64) ENGINE = Memory;"

function thread1()
{
        for _ in {1..10}
        do
                seq 1 500000 | $CLICKHOUSE_CLIENT --query_id=11 --query="INSERT INTO data_a_01019 FORMAT TSV" &
                while clickhouse_test_time_left; do
                        $CLICKHOUSE_CLIENT --query="KILL QUERY WHERE query_id='11' SYNC" | grep -q "cant_cancel" && sleep .1 || break ||:
                done
                clickhouse_test_time_left || return 1
                while clickhouse_test_time_left; do
                        $CLICKHOUSE_CLIENT --query="SELECT count(*)>0 FROM system.processes WHERE query_id='11'" | grep -q "1" && sleep .1 || break ||:
                done
                clickhouse_test_time_left || return 1
        done
        return 0
}

function thread2()
{
        for _ in {1..10}
        do
                seq 1 500000 | $CLICKHOUSE_CLIENT --query_id=22 --query="INSERT INTO data_b_01019 FORMAT TSV" &
                while clickhouse_test_time_left; do
                        $CLICKHOUSE_CLIENT --query="KILL QUERY WHERE query_id='22' SYNC" | grep -q "cant_cancel" && sleep .1 || break ||:
                done
                clickhouse_test_time_left || return 1
                while clickhouse_test_time_left; do
                        $CLICKHOUSE_CLIENT --query="SELECT count(*)>0 FROM system.processes WHERE query_id='22'" | grep -q "1" && sleep .1 || break ||:
                done
                clickhouse_test_time_left || return 1
        done
        return 0
}

export -f thread1;
export -f thread2;

bash -c thread1 > /dev/null 2>&1 &
pid1=$!
bash -c thread2 > /dev/null 2>&1 &
pid2=$!

# A worker exits with an error when the test deadline expires before its polling loops finish.
status=0
wait "$pid1" || status=1
wait "$pid2" || status=1
if [ "$status" = 0 ]
then
    echo OK
else
    echo "FAIL: a worker did not finish before the test deadline"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE data_a_01019"
$CLICKHOUSE_CLIENT --query "DROP TABLE data_b_01019"
