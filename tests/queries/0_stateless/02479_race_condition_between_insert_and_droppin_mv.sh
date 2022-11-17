#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function insert {
    x=0
    i=0
    offset=500
    while [ $x -le $1 ]
    do
        {
            ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_race_condition_landing SELECT number, toString(number), toString(number) from system.numbers limit $i, $offset"
        } || {
            for thread_pid in ${$2[@]}; do
                kill $thread_pid
            done
        }
        i=$(( $i + $RANDOM % 100 + 400 ))
        x=$(( $x + 1 ))
    done
}

function drop_mv {
    index=$1
    while true;
    do
        ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv_$index"
        ${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW IF NOT EXISTS test_race_condition_mv1_$index TO test_race_condition_target AS select count() as number FROM (SELECT a.number, a.n, a.n2, b.number, b.n, b.n2, c.number, c.n, c.n2 FROM test_race_condition_landing a CROSS JOIN test_race_condition_landing b CROSS JOIN test_race_condition_landing c)"
        ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv1_$index"
        ${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW IF NOT EXISTS test_race_condition_mv_$index TO test_race_condition_target AS select count() as number FROM (SELECT a.number, a.n, a.n2, b.number, b.n, b.n2, c.number, c.n, c.n2 FROM test_race_condition_landing a CROSS JOIN test_race_condition_landing b CROSS JOIN test_race_condition_landing c)"
    done
}

${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_race_condition_target (number Int64) Engine=MergeTree ORDER BY number"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_race_condition_landing (number Int64, n String, n2 String) Engine=MergeTree ORDER BY number"

drop_mv_threads=()
for i in {1..3}
do
    drop_mv $i &
    drop_mv_pid=$!
    drop_mv_threads+=($drop_mv_pid)
done

insert_threads=()
for i in {1..3}
do
    insert 100 drop_mv_threads &
    insert_pid=$!
    insert_threads+=($insert_pid)
done


for thread_pid in ${insert_threads[@]}; do
    wait $thread_pid
done
for thread_pid in ${drop_mv_threads[@]}; do
    kill $thread_pid
done


${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_landing"
for i in {1..3}
do
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv_$i"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv1_$i"
done


echo "PASSED"
