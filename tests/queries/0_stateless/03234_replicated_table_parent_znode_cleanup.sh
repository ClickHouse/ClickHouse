#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -nq "
    create database $db engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1');
    create table $db.a (x Int8) engine ReplicatedMergeTree order by x;"
uuid=`$CLICKHOUSE_CLIENT -q "select uuid from system.tables where database = '$db' and name = 'a'"`
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -nq "
    select count() from system.zookeeper where path = '/clickhouse/tables' and name = '$uuid';
    drop table $db.a sync;
    select count() from system.zookeeper where path = '/clickhouse/tables' and name = '$uuid';"
$CLICKHOUSE_CLIENT -q "drop database $db"
