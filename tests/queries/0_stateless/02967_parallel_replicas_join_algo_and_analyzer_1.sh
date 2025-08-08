#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -m -q "
drop table if exists num_1;
drop table if exists num_2;

create table num_1 (key UInt64, value String) engine = MergeTree order by key;
create table num_2 (key UInt64, value Int64) engine = MergeTree order by key;

insert into num_1 select number * 2, toString(number * 2) from numbers(1e7);
insert into num_2 select number * 3, -number from numbers(1.5e6);
"

##############
echo
echo "simple join with analyzer"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2) r on l.key = r.key
order by l.key limit 10 offset 700000
SETTINGS enable_analyzer=1"

PARALLEL_REPLICAS_SETTINGS="enable_parallel_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_prefer_local_join = 0"

##############
echo
echo "simple (global) join with analyzer and parallel replicas"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2) r on l.key = r.key
order by l.key limit 10 offset 700000
SETTINGS enable_analyzer=1, $PARALLEL_REPLICAS_SETTINGS, parallel_replicas_local_plan=0"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2) r on l.key = r.key
order by l.key limit 10 offset 700000
SETTINGS enable_analyzer=1, send_logs_level='trace', $PARALLEL_REPLICAS_SETTINGS, parallel_replicas_local_plan=0" 2>&1 |
grep "Executing read from.*with parallel replicas\|<Trace>.*Coordinator: Coordination done" |
grep -o "(SELECT.*WithMergeableState\|<Trace>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'

echo
echo "simple (global) join with analyzer and parallel replicas with local plan"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2) r on l.key = r.key
order by l.key limit 10 offset 700000
SETTINGS enable_analyzer=1, $PARALLEL_REPLICAS_SETTINGS, parallel_replicas_local_plan=1"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2) r on l.key = r.key
order by l.key limit 10 offset 700000
SETTINGS enable_analyzer=1, send_logs_level='trace', $PARALLEL_REPLICAS_SETTINGS, parallel_replicas_local_plan=1" 2>&1 |
grep "Executing read from.*with parallel replicas\|<Trace>.*Coordinator: Coordination done" |
grep -o "(SELECT.*WithMergeableState\|<Trace>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'
