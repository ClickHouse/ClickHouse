#!/usr/bin/env bash

# NOTE: sh test is required since view() does not have current database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
drop table if exists dst_02225;
drop table if exists src_02225;
create table dst_02225 (key Int) engine=Memory();
create table src_02225 (key Int) engine=Memory();
insert into src_02225 values (1);
"

$CLICKHOUSE_CLIENT --param_database=$CLICKHOUSE_DATABASE -m -q "
truncate table dst_02225;
insert into function remote('127.{1,2}', currentDatabase(), dst_02225, key)
select * from remote('127.{1,2}', view(select * from {database:Identifier}.src_02225), key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02225;

-- w/o sharding key
truncate table dst_02225;
insert into function remote('127.{1,2}', currentDatabase(), dst_02225, key)
select * from remote('127.{1,2}', view(select * from {database:Identifier}.src_02225))
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02225;
"

$CLICKHOUSE_CLIENT -m -q "
drop table src_02225;
drop table dst_02225;
"
