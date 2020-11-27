#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo "DROP TABLE IF EXISTS tab_00738;
DROP TABLE IF EXISTS mv;
CREATE TABLE tab_00738(a Int) ENGINE = Log;
CREATE MATERIALIZED VIEW mv ENGINE = Log AS SELECT a FROM tab_00738;" | ${CLICKHOUSE_CLIENT} -n

${CLICKHOUSE_CLIENT} --query_id test_00738 --query "INSERT INTO tab_00738 SELECT number FROM numbers(10000000)" &

function drop()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE \`.inner.mv\`" -n
}

function wait_for_query_to_start()
{
    while [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = 'test_00738'") == 0 ]]; do sleep 0.001; done
}

export -f wait_for_query_to_start
timeout 5 bash -c wait_for_query_to_start

drop &

wait

echo "DROP TABLE IF EXISTS tab_00738;
DROP TABLE IF EXISTS mv;" | ${CLICKHOUSE_CLIENT} -n
