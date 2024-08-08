#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -nq "
    create table a (x Int64) engine MergeTree order by x;
    insert into a values (1), (2);"

# Run for 10 seconds.
start_time=$(date +%s)
end_time=$((start_time + 10))

function exchange_thread()
{
    while [ "$(date +%s)" -lt $end_time ]
    do
        $CLICKHOUSE_CLIENT -nq "
            create table t empty as a;
            insert into t values (1), (2);
            exchange tables a and t;
            drop table t;"
    done
}

function select_thread()
{
    while [ "$(date +%s)" -lt $end_time ]
    do
        r=`$CLICKHOUSE_CLIENT -q "select count(), sum(x) from a format CSV"`
        if [ "$r" != "2,3" ]
        then
            echo "unexpected select result: $r"
            break
        fi
    done
}

exchange_thread &
select_thread &

wait

echo "done"
