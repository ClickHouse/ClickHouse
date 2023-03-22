#!/usr/bin/env bash
# Tags: long, zookeeper, race, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create user u_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "grant all on db_$CLICKHOUSE_DATABASE.* to u_$CLICKHOUSE_DATABASE"


function thread_ddl()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "create database if not exists db_$CLICKHOUSE_DATABASE"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE if not exists db_$CLICKHOUSE_DATABASE.test (test String, A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') ORDER BY tuple();"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') partition by A order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        sleep 0.$RANDOM;
        sleep 0.$RANDOM;
        $CLICKHOUSE_CLIENT -q "drop database if exists db_$CLICKHOUSE_DATABASE"
    done
}

function thread_insert()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO db_$CLICKHOUSE_DATABASE.test SELECT 'case1', number%3, rand() FROM numbers(5)"
        sleep 0.$RANDOM;
    done
}

function thread_restart()
{
    while true; do
        # The simplest way to restart only replicas from a specific database is to use a special user
        $CLICKHOUSE_CLIENT --user "u_$CLICKHOUSE_DATABASE" -q "system restart replicas"
        sleep 0.$RANDOM;
    done
}

export -f thread_ddl;
export -f thread_insert;
export -f thread_restart;

TIMEOUT=30

timeout $TIMEOUT bash -c thread_ddl 2>&1| grep -Fa "Exception: " | grep -Fv -e "TABLE_IS_DROPPED" -e "UNKNOWN_TABLE" -e "DATABASE_NOT_EMPTY" &
timeout $TIMEOUT bash -c thread_insert 2> /dev/null &
timeout $TIMEOUT bash -c thread_restart &

wait

$CLICKHOUSE_CLIENT -q "drop database if exists db_$CLICKHOUSE_DATABASE" 2>&1| grep -Fa "Exception: " | grep -Fv -e "TABLE_IS_DROPPED" -e "UNKNOWN_TABLE" -e "DATABASE_NOT_EMPTY" ||:
