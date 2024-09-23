#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function create_db()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        SHARD=$(($RANDOM % 2))
        REPLICA=$(($RANDOM % 2))
        SUFFIX=$(($RANDOM % 16))
        # Multiple database replicas on one server are actually not supported (until we have namespaces).
        # So CREATE TABLE queries will fail on all replicas except one. But it's still makes sense for a stress test.
        $CLICKHOUSE_CLIENT --query \
        "create database if not exists ${CLICKHOUSE_DATABASE}_repl_01111_$SUFFIX engine=Replicated('/test/01111/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '$SHARD', '$REPLICA')" \
         2>&1| grep -Fa "Exception: " | grep -Fv "REPLICA_ALREADY_EXISTS" | grep -Fiv "Will not try to start it up" | \
         grep -Fv "Coordination::Exception" | grep -Fv "already contains some data and it does not look like Replicated database path"
        sleep 0.$RANDOM
    done
}

function drop_db()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [[ "$database" == "$CLICKHOUSE_DATABASE" ]]; then continue; fi
        if [ -z "$database" ]; then continue; fi
        $CLICKHOUSE_CLIENT --query \
        "drop database if exists $database" 2>&1| grep -Fa "Exception: "
        sleep 0.$RANDOM
    done
}

function sync_db()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$database" ]; then continue; fi
        $CLICKHOUSE_CLIENT --receive_timeout=1 -q \
        "system sync database replica $database" 2>&1| grep -Fa "Exception: " | grep -Fv TIMEOUT_EXCEEDED | grep -Fv "only with Replicated engine" | grep -Fv UNKNOWN_DATABASE
        sleep 0.$RANDOM
    done
}

function create_table()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        database=$($CLICKHOUSE_CLIENT -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$database" ]; then continue; fi
        $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
        "create table $database.rmt_${RANDOM}_${RANDOM}_${RANDOM} (n int) engine=ReplicatedMergeTree order by tuple() -- suppress $CLICKHOUSE_TEST_ZOOKEEPER_PREFIX" \
        2>&1| grep -Fa "Exception: " | grep -Fv "Macro 'uuid' and empty arguments" | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE | grep -Fv TABLE_IS_DROPPED
        sleep 0.$RANDOM
    done
}

function alter_table()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$table" ]; then continue; fi
        $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 -q \
        "alter table $table update n = n + (select max(n) from merge(REGEXP('${CLICKHOUSE_DATABASE}.*'), '.*')) where 1 settings allow_nondeterministic_mutations=1" \
        2>&1| grep -Fa "Exception: " | grep -Fv "Cannot enqueue query" | grep -Fv "ZooKeeper session expired" | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY | grep -Fv TABLE_IS_DROPPED | grep -Fv ABORTED | grep -Fv "Error while executing table function merge"
        sleep 0.$RANDOM
    done
}

function insert()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        table=$($CLICKHOUSE_CLIENT -q "select database || '.' || name from system.tables where database like '${CLICKHOUSE_DATABASE}%' order by rand() limit 1")
        if [ -z "$table" ]; then continue; fi
        $CLICKHOUSE_CLIENT -q \
        "insert into $table values ($RANDOM)" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_DATABASE | grep -Fv UNKNOWN_TABLE | grep -Fv TABLE_IS_READ_ONLY | grep -Fv TABLE_IS_DROPPED
    done
}



TIMEOUT=20

create_db $TIMEOUT &
sync_db $TIMEOUT &
create_table $TIMEOUT &
alter_table $TIMEOUT &
insert $TIMEOUT &

sleep 1 # give other queries a head start
drop_db $TIMEOUT &

wait

readarray -t databases_arr < <(${CLICKHOUSE_CLIENT} -q "select name from system.databases where name like '${CLICKHOUSE_DATABASE}_%'")
for db in "${databases_arr[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop database if exists $db"
done
