#!/usr/bin/env bash
# Tags: no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db_name=$(tr -dc A-Za-z </dev/urandom | head -c 10; echo)

$CLICKHOUSE_CLIENT --query "create database ${db_name} engine = Atomic"

id=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --query "create table ${db_name}.t1 (a Int)
                                engine=ReplicatedMergeTree('/clickhouse/tables/{database}/${id}', '{replica}')
                                order by tuple() SETTINGS index_granularity = 8192"

# -m1 because it contains stack trace as well
$CLICKHOUSE_CLIENT --query "attach table ${db_name}.t2 UUID '${id}'
                                   (a Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/${id}', '{replica}')
                                   order by tuple() SETTINGS index_granularity = 8192" 2>&1 | grep -m 1 -o -F 'REPLICA_ALREADY_EXISTS'

$CLICKHOUSE_CLIENT --query "drop database ${db_name} sync"
