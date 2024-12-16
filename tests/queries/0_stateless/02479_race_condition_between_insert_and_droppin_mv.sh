#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, long

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="error"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function insert {
    i=0
    offset=500
    while true;
    do
        ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_race_condition_landing SELECT number, toString(number), toString(number) from system.numbers limit $i, $offset settings ignore_materialized_views_with_dropped_target_table=1"
        i=$(( $i + $RANDOM % 100 + 400 ))
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

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_landing"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_race_condition_target (number Int64) Engine=MergeTree ORDER BY number"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_race_condition_landing (number Int64, n String, n2 String) Engine=MergeTree ORDER BY number"

export -f drop_mv;
export -f insert;

TIMEOUT=50

for i in {1..4}
do
    timeout $TIMEOUT bash -c "drop_mv $i" &
done

for i in {1..4}
do
    timeout $TIMEOUT bash -c insert 20 &
done

wait


${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_landing"
for i in {1..4}
do
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv_$i"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_race_condition_mv1_$i"
done


echo "PASSED"
