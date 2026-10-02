-- Tags: distributed

set optimize_skip_unused_shards=1;
set optimize_distributed_group_by_sharding_key=1;
set prefer_localhost_replica=1;

set enable_analyzer = 0;

-- { echo }
explain select distinct k1 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- not optimized
explain select distinct k1, k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- optimized
explain select distinct on (k1) k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- not optimized
explain select distinct on (k1, k2) v from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- optimized

explain select distinct k1 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- not optimized
explain select distinct k1, k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- optimized
explain select distinct on (k1) k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- not optimized
explain select distinct on (k1, k2) v from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- optimized

set enable_analyzer = 1;

explain select distinct k1 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- not optimized
explain select distinct k1, k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- optimized
explain select distinct on (k1) k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- not optimized
explain select distinct on (k1, k2) v from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)); -- optimized

explain select distinct k1 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- not optimized
explain select distinct k1, k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- optimized
explain select distinct on (k1) k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- not optimized
explain select distinct on (k1, k2) v from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v from numbers(2)), cityHash64(k1, k2)) order by v; -- optimized
