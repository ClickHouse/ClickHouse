#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "create table src (n int) engine=MergeTree order by n"
$CLICKHOUSE_CLIENT --query "create table dst (n int) engine=MergeTree order by n"
$CLICKHOUSE_CLIENT --query "create materialized view mv to dst (n int) as select * from src"
$CLICKHOUSE_CLIENT --query "insert into src select 1"
$CLICKHOUSE_CLIENT --query "select sleep(2) from mv format Null" 2> /dev/null &
sleep 0.$RANDOM

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "drop table mv"
$CLICKHOUSE_CLIENT --query "create materialized view mv to dst (n int) as select * from src"

wait
sleep 1 # increase chance of reproducing the issue
$CLICKHOUSE_CLIENT --query "insert into src select 2"

$CLICKHOUSE_CLIENT --query "select * from dst order by n"

$CLICKHOUSE_CLIENT --query "drop table mv"
$CLICKHOUSE_CLIENT --query "drop table src"
$CLICKHOUSE_CLIENT --query "drop table dst"
