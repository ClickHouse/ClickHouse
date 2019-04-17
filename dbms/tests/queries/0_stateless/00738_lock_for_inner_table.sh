#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo "DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS mv;
CREATE TABLE tab(a Int) ENGINE = Log;
CREATE MATERIALIZED VIEW mv ENGINE = Log AS SELECT a FROM tab;" | ${CLICKHOUSE_CLIENT} -n

${CLICKHOUSE_CLIENT} --query "INSERT INTO tab SELECT number FROM numbers(10000000)" &

function drop()
{
    sleep 0.1
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE \`.inner.mv\`" -n
}

drop &

wait

echo "DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS mv;" | ${CLICKHOUSE_CLIENT} -n 
