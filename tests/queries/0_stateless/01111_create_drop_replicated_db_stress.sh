#!/usr/bin/env bash
# Tags: race, zookeeper, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function create_db()
{
    while true; do
        SHARD=$(($RANDOM % 2))
        REPLICA=$(($RANDOM % 2))
        SUFFIX=$(($RANDOM % 16))
        # Multiple database replicas on one server are actually not supported (until we have namespaces).
        # So CREATE TABLE queries will fail on all replicas except one. But it's still makes sense for a stress test.
        $CLICKHOUSE_CLIENT --allow_experimental_database_replicated=1 --query \
        "create database if not exists ${CLICKHOUSE_DATABASE}_repl_$SUFFIX engine=Replicated('/test/01111/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '$SHARD', '$REPLICA')" \
         2>&1| grep -Fa "Exception: " | grep -Fv "REPLICA_IS_ALREADY_EXIST" | grep -Fiv "Will not try to start it up" | \
         grep -Fv "Coordination::Exception" | grep -Fv "already contains some data and it does not look like Replicated database path"
        sleep 0.$RANDOM
    done
}

function drop_db()
{
    while true; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [[ "$database" == "$CLICKHOUSE_DATABASE" ]]; then return; fi
        if [ -z "$database" ]; then return; fi
        $CLICKHOUSE_CLIENT -n --query \
        "drop database if exists $database" 2>&1| grep -Fa "Exception: "
        sleep 0.$RANDOM
    done
}

function sync_db()
{
    while true; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$database" ]; then return; fi
        $CLICKHOUSE_CLIENT --receive_timeout=1 -q \
        "system sync database replica $database" 2>&1| grep -Fa "Exception: " | grep -Fv TIMEOUT_EXCEEDED | grep -Fv "only with Replicated engine" | grep -Fv UNKNOWN_DATABASE
        sleep 0.$RANDOM
    done
}

function create_table()
{
    while true; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$database" ]; then return; fi
        $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
        "create table $database.rmt_$RANDOM (n int) engine=ReplicatedMergeTree order by tuple() -- suppress $CLICKHOUSE_TEST_ZOOKEEPER_PREFIX" \
        2>&1| grep -Fa "Exception: " | grep -Fv "Macro 'uuid' and empty arguments" | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE
        sleep 0.$RANDOM
    done
}

function alter_table()
{
    while true; do
        table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$table" ]; then return; fi
        $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
        "alter table $table on cluster $database update n = n + (select max(n) from merge(REGEXP('${CLICKHOUSE_DATABASE}.*'), '.*')) where 1 settings allow_nondeterministic_mutations=1" \
        2>&1| grep -Fa "Exception: " | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY
        sleep 0.$RANDOM
    done
}

function insert()
{
    while true; do
        table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$table" ]; then return; fi
        $CLICKHOUSE_CLIENT -q \
        "insert into $table values ($RANDOM)" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY
    done
}



export -f create_db
export -f drop_db
export -f sync_db
export -f create_table
export -f alter_table
export -f insert

TIMEOUT=30

timeout $TIMEOUT bash -c create_db &
timeout $TIMEOUT bash -c sync_db &
timeout $TIMEOUT bash -c create_table &
timeout $TIMEOUT bash -c alter_table &
timeout $TIMEOUT bash -c insert &

sleep 1 # give other queries a head start
timeout $TIMEOUT bash -c drop_db &

wait

readarray -t databases_arr < <(${CLICKHOUSE_CLIENT} -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}_%'")
for db in "${databases_arr[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop database if exists $db"
done
