#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db_name=$(tr -dc A-Za-z </dev/urandom | head -c 10; echo)

$CLICKHOUSE_CLIENT --query "drop database if exists ${db_name}"
$CLICKHOUSE_CLIENT --query "create database ${db_name} engine = Atomic"

$CLICKHOUSE_CLIENT --query "drop table if exists ${db_name}.t1 sync"
$CLICKHOUSE_CLIENT --query "drop table if exists ${db_name}.t2 sync"

id=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --query "create table ${db_name}.t1 (a Int)
                                engine=ReplicatedMergeTree('/clickhouse/tables/{database}/${id}', '{replica}')
                                order by tuple() SETTINGS index_granularity = 8192"
$CLICKHOUSE_CLIENT --send_logs_level fatal --query "attach table ${db_name}.t2 UUID '${id}'
                                   (a Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/${id}', '{replica}')
                                   order by tuple() SETTINGS index_granularity = 8192" 2>&1 | grep -o 'REPLICA_ALREADY_EXISTS'
