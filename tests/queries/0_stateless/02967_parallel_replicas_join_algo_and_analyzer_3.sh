#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -nm -q "
drop table if exists num_1;
drop table if exists num_2;

create table num_1 (key UInt64, value String) engine = MergeTree order by key;
create table num_2 (key UInt64, value Int64) engine = MergeTree order by key;

insert into num_1 select number * 2, toString(number * 2) from numbers(1e7);
insert into num_2 select number * 3, -number from numbers(1.5e6);
"

##############
echo
echo "nested join with analyzer and parallel replicas, both global"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings parallel_replicas_prefer_local_join=0) r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1,
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_prefer_local_join=0"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings parallel_replicas_prefer_local_join=0) r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, send_logs_level='trace',
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_prefer_local_join=0" 2>&1 |
grep "executeQuery\|<Debug>.*Coordinator: Coordination done" |
grep -o "SELECT.*WithMergeableState)\|<Debug>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'

##############
echo
echo "nested join with analyzer and parallel replicas, global + local"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings parallel_replicas_prefer_local_join=1) r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1,
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_prefer_local_join=0"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings parallel_replicas_prefer_local_join=1) r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, send_logs_level='trace',
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_prefer_local_join=0" 2>&1 |
grep "executeQuery\|<Debug>.*Coordinator: Coordination done" |
grep -o "SELECT.*WithMergeableState)\|<Debug>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'


##############
echo
echo "nested join with analyzer and parallel replicas, both local, both full sorting merge join"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='full_sorting_merge') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0,
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='full_sorting_merge'"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='full_sorting_merge') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0, send_logs_level='trace',
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='full_sorting_merge'" 2>&1 |
grep "executeQuery\|<Debug>.*Coordinator: Coordination done" |
grep -o "SELECT.*WithMergeableState)\|<Debug>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'

##############
echo
echo "nested join with analyzer and parallel replicas, both local, both full sorting and hash join"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='hash') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0,
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='full_sorting_merge'"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='hash') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0, send_logs_level='trace',
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='full_sorting_merge'" 2>&1 |
grep "executeQuery\|<Debug>.*Coordinator: Coordination done" |
grep -o "SELECT.*WithMergeableState)\|<Debug>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'

##############
echo
echo "nested join with analyzer and parallel replicas, both local, both full sorting and hash join"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='full_sorting_merge') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0,
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='hash'"

$CLICKHOUSE_CLIENT -q "
select * from (select key, value from num_1) l
inner join (select key, value from num_2 inner join
  (select number * 7 as key from numbers(1e5)) as nn on num_2.key = nn.key settings join_algorithm='full_sorting_merge') r
on l.key = r.key order by l.key limit 10 offset 10000
SETTINGS enable_analyzer=1, parallel_replicas_prefer_local_join=0, send_logs_level='trace',
allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', join_algorithm='hash'" 2>&1 |
grep "executeQuery\|<Debug>.*Coordinator: Coordination done" |
grep -o "SELECT.*WithMergeableState)\|<Debug>.*Coordinator: Coordination done" |
sed -re 's/_data_[[:digit:]]+_[[:digit:]]+/_data_/g'
