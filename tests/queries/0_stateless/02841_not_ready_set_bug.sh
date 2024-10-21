#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists t1;"
$CLICKHOUSE_CLIENT -q "create table t1 (number UInt64) engine = MergeTree order by tuple();"
$CLICKHOUSE_CLIENT -q "insert into t1 select number from numbers(10);"
$CLICKHOUSE_CLIENT --max_threads=2 --max_result_rows=1 --result_overflow_mode=break -q "with tab as (select min(number) from t1 prewhere number in (select number from view(select number, row_number() OVER (partition by number % 2 ORDER BY number DESC) from numbers_mt(1e4)) where number != 2 order by number)) select number from t1 union all select * from tab;" > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT * FROM system.tables WHERE 1 in (SELECT number from numbers(2)) AND database = currentDatabase() format Null"
$CLICKHOUSE_CLIENT -q "SELECT xor(1, 0) FROM system.parts WHERE 1 IN (SELECT 1) FORMAT Null"

# (Not all of these tests are effective because some of these tables are empty.)
$CLICKHOUSE_CLIENT -nq "
    select * from system.columns where table in (select '123');
    select * from system.replicas where database in (select '123');
    select * from system.data_skipping_indices where database in (select '123');
    select * from system.databases where name in (select '123');
    select * from system.mutations where table in (select '123');
    select * from system.part_moves_between_shards where database in (select '123');
    select * from system.replication_queue where database in (select '123');
    select * from system.distribution_queue where database in (select '123');
"
$CLICKHOUSE_CLIENT -nq "
    create table a (x Int8) engine MergeTree order by x;
    insert into a values (1);
    select * from mergeTreeIndex(currentDatabase(), 'a') where part_name in (select '123');
"
