#!/usr/bin/env bash

# Tags: no-fasttest
# no-fasttest: Slow timeouts

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_$CLICKHOUSE_DATABASE"
db2="${db}_2"
db3="${db}_3"

$CLICKHOUSE_CLIENT -q "create database $db engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1')"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "create table $db.mt (n int) engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "create table $db.rmt (n int) engine=ReplicatedMergeTree order by tuple()"

$CLICKHOUSE_CLIENT -q "insert into $db.rmt values (0), (1)"
$CLICKHOUSE_CLIENT -q "insert into $db.mt values (0), (1)"

$CLICKHOUSE_CLIENT -q "create database $db2 engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r2')"
$CLICKHOUSE_CLIENT -q "create database $db3 engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's2', 'r1')"

$CLICKHOUSE_CLIENT -q "alter table $db.mt drop partition id 'all', add column m int" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1
$CLICKHOUSE_CLIENT -q "alter table $db.rmt drop partition id 'all', add column m int" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1

$CLICKHOUSE_CLIENT -q "alter table $db.mt drop partition id 'all', update n = 2 where 1" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1
$CLICKHOUSE_CLIENT -q "alter table $db.rmt drop partition id 'all', update n = 2 where 1" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "delete from $db.mt where n=2" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "delete from $db.rmt where n=2" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "alter table $db.mt update n=2 where n=3" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "alter table $db.rmt update n=2 where n=3" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1

$CLICKHOUSE_CLIENT -q "drop database $db3"
# now there's only one shard

$CLICKHOUSE_CLIENT -q "alter table $db.mt drop partition id 'all', add column m int" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1
$CLICKHOUSE_CLIENT -q "alter table $db.rmt drop partition id 'all', add column m int" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1

$CLICKHOUSE_CLIENT -q "alter table $db.mt drop partition id 'all', update n = 2 where 1" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1
$CLICKHOUSE_CLIENT -q "alter table $db.rmt drop partition id 'all', update n = 2 where 1" 2>&1| grep -Eo "not allowed to execute ALTERs of different types" | head -1

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "alter table $db.mt update n=2 where n=1" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1
$CLICKHOUSE_CLIENT -q "alter table $db.rmt update n=2 where n=1 settings mutations_sync=1"

$CLICKHOUSE_CLIENT -q "select 1, * from $db.rmt order by n"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "delete from $db.mt where n=2" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "delete from $db.rmt where n=2"

$CLICKHOUSE_CLIENT -q "select 2, * from $db.rmt order by n"

$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "delete from $db.mt where n=2" 2>&1| grep -Eo "TIMEOUT_EXCEEDED" | head -1
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=3 -q "alter table $db.rmt attach partition id 'all' from $db.mt"

$CLICKHOUSE_CLIENT -q "select 3, * from $db.rmt order by n"

$CLICKHOUSE_CLIENT -q "drop database $db2"
$CLICKHOUSE_CLIENT -q "drop database $db"
