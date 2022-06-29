#!/usr/bin/env bash
# Tags: race, zookeeper, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function create_db()
{
    SHARD=$(($RANDOM % 2))
    REPLICA=$(($RANDOM % 2))
    SUFFIX=$(($RANDOM % 16))
    # Multiple database replicas on one server are actually not supported (until we have namespaces).
    # So CREATE TABLE queries will fail on all replicas except one. But it's still makes sense for a stress test.
    $CLICKHOUSE_CLIENT --allow_experimental_database_replicated=1 --query \
    "create database if not exists ${CLICKHOUSE_DATABASE}_repl_$SUFFIX engine=Replicated('/test/01111/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '$SHARD', '$REPLICA')" \
     2>&1| grep -Fa "Exception: " | grep -Fv "REPLICA_IS_ALREADY_EXIST" | grep -Fiv "Will not try to start it up" | grep -Fv "Coordination::Exception"
    sleep 0.$RANDOM
}

function drop_db()
{
    database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
    if [[ "$database" == "$CLICKHOUSE_DATABASE" ]]; then return; fi
    if [ -z "$database" ]; then return; fi
    $CLICKHOUSE_CLIENT -n --query \
    "drop database if exists $database" 2>&1| grep -Fa "Exception: "
    sleep 0.$RANDOM
}

function sync_db()
{
    database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
    if [ -z "$database" ]; then return; fi
    $CLICKHOUSE_CLIENT --receive_timeout=1 -q \
    "system sync database replica $database" 2>&1| grep -Fa "Exception: " | grep -Fv TIMEOUT_EXCEEDED | grep -Fv "only with Replicated engine" | grep -Fv UNKNOWN_DATABASE
    sleep 0.$RANDOM
}

function create_table()
{
    database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
    if [ -z "$database" ]; then return; fi
    $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
    "create table $database.rmt_$RANDOM (n int) engine=ReplicatedMergeTree order by tuple() -- suppress CLICKHOUSE_TEST_ZOOKEEPER_PREFIX" \
    2>&1| grep -Fa "Exception: " | grep -Fv "Macro 'uuid' and empty arguments" | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE
    sleep 0.$RANDOM
}

function alter_table()
{
    table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
    if [ -z "$table" ]; then return; fi
    $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
    "alter table $table update n = n + (select max(n) from merge(REGEXP('${CLICKHOUSE_DATABASE}.*'), '.*')) where 1 settings allow_nondeterministic_mutations=1" \
    2>&1| grep -Fa "Exception: " | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY
    sleep 0.$RANDOM
}

function insert()
{
    table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
    if [ -z "$table" ]; then return; fi
    $CLICKHOUSE_CLIENT -q \
    "insert into $table values ($RANDOM)" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY
}



export -f create_db
export -f drop_db
export -f sync_db
export -f create_table
export -f alter_table
export -f insert

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT create_db &
clickhouse_client_loop_timeout $TIMEOUT sync_db &
clickhouse_client_loop_timeout $TIMEOUT create_table &
clickhouse_client_loop_timeout $TIMEOUT alter_table &
clickhouse_client_loop_timeout $TIMEOUT insert &

sleep 1 # give other queries a head start
clickhouse_client_loop_timeout $TIMEOUT drop_db &

wait

readarray -t databases_arr < <(${CLICKHOUSE_CLIENT} -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}_%'")
for db in "${databases_arr[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop database $db"
done
