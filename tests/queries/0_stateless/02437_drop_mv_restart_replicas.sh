#!/usr/bin/env bash
# Tags: long, zookeeper, race, no-ordinary-database, no-replicated-database
# FIXME remove no-replicated-database tag

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create user u_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "grant all on db_$CLICKHOUSE_DATABASE.* to u_$CLICKHOUSE_DATABASE"

# For tests with Replicated
ENGINE=$($CLICKHOUSE_CLIENT -q "select replace(engine_full, '$CLICKHOUSE_DATABASE', 'db_$CLICKHOUSE_DATABASE') from system.databases where name='$CLICKHOUSE_DATABASE' format TSVRaw")
export ENGINE

function thread_ddl()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "create database if not exists db_$CLICKHOUSE_DATABASE engine=$ENGINE"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE if not exists db_$CLICKHOUSE_DATABASE.test (test String, A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') ORDER BY tuple();"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') partition by A order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW if not exists db_$CLICKHOUSE_DATABASE.test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1') order by tuple() AS SELECT test, A, count() c FROM db_$CLICKHOUSE_DATABASE.test group by test, A;"
        sleep 0.$RANDOM;

        # A kind of backoff
        timeout 5s $CLICKHOUSE_CLIENT -q "select sleepEachRow(0.1) from system.dropped_tables format Null" 2>/dev/null ||:

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

TIMEOUT=15

timeout $TIMEOUT bash -c thread_ddl 2>&1| grep -Fa "Exception: " | grep -Fv -e "TABLE_IS_DROPPED" -e "UNKNOWN_TABLE" -e "DATABASE_NOT_EMPTY" -e "TABLE_IS_BEING_RESTARTED" &
timeout $TIMEOUT bash -c thread_insert 2> /dev/null &
timeout $TIMEOUT bash -c thread_restart 2>&1| grep -Fa "Exception: " | grep -Fv -e "is currently dropped or renamed" -e "is being dropped or detached" &

wait

timeout 45s $CLICKHOUSE_CLIENT -q "select sleepEachRow(0.3) from system.dropped_tables format Null" 2>/dev/null ||:

$CLICKHOUSE_CLIENT -q "drop database if exists db_$CLICKHOUSE_DATABASE" 2>&1| grep -Fa "Exception: " | grep -Fv -e "TABLE_IS_DROPPED" -e "UNKNOWN_TABLE" -e "DATABASE_NOT_EMPTY" ||:
