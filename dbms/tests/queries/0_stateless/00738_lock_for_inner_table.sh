#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo "DROP TABLE IF EXISTS test.tab;
DROP TABLE IF EXISTS test.mv;
CREATE TABLE test.tab(a Int) ENGINE = Log;
CREATE MATERIALIZED VIEW test.mv ENGINE = Log AS SELECT a FROM test.tab;" | ${CLICKHOUSE_CLIENT} -n

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.tab SELECT number FROM numbers(10000000)" &

function drop()
{
    sleep 0.1
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE test.\`.inner.mv\`" -n
}

drop &

wait
