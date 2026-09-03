#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "select 1; select 1 union all (select 1 union distinct select 1);   " | $CLICKHOUSE_FORMAT -n;

echo "select 1; select 1 union all (select 1 union distinct select 1); -- comment  " | $CLICKHOUSE_FORMAT -n;

echo "insert into t values (1); " | $CLICKHOUSE_FORMAT -n

echo 'insert into t format JSONEachRow {"a":1};' | $CLICKHOUSE_FORMAT -n  2>&1 \ | grep -F -q "NOT_IMPLEMENTED" && echo 'OK' || echo 'FAIL'
