#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="
    DROP TABLE IF EXISTS test.mv_test;
    DROP TABLE IF EXISTS test.mv_test_src;
    CREATE TABLE test.mv_test     (date Date, value String) Engine = MergeTree(date, (date), 8192);
    CREATE TABLE test.mv_test_src (date Date, value String) Engine = MergeTree(date, (date), 8192);
    CREATE MATERIALIZED VIEW test.mv_mv_test TO test.mv_test AS SELECT * FROM test.mv_test_src;
    ALTER TABLE test.mv_mv_test DROP PARTITION 201801;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "236" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

$CLICKHOUSE_CLIENT --query="DROP TABLE test.mv_mv_test;";