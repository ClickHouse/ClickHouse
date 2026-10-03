-- Tags: shard

-- Follow-up to https://github.com/ClickHouse/ClickHouse/pull/110993: `max_temporary_data_on_disk_size_for_query`
-- was not enforced on join spill and on aggregation spill of deserialized query plans, because in both places the
-- spill scope was created from the server-global temporary data scope instead of the scope of the query context
-- that carries the per-query and per-user limits.

-- Grace hash join always writes the non-current buckets to disk, so the spill is deterministic.
select 'join spill without per-query limit works';
select count()
from (select number as k, toString(cityHash64(number)) as v from numbers(300000)) as t1
inner join (select number as k, toString(cityHash64(number + 1)) as v from numbers(300000)) as t2
on t1.k = t2.k
format Null
settings enable_analyzer = 1, join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4,
         max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 1;

select count()
from (select number as k, toString(cityHash64(number)) as v from numbers(300000)) as t1
inner join (select number as k, toString(cityHash64(number + 1)) as v from numbers(300000)) as t2
on t1.k = t2.k
format Null
settings enable_analyzer = 1, join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4,
         max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 1,
         max_temporary_data_on_disk_size_for_query = 100000; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- The remote workers receive a serialized query plan (`serialize_query_plan`) and deserialize the partial
-- aggregation step on it, taking the spill scope on the plan-deserialization path.
drop table if exists t_04625;
create table t_04625 (n UInt64) engine = MergeTree order by n;
insert into t_04625 select number from numbers(500000);

select 'aggregation spill on remote plan without per-query limit works';
select toString(cityHash64(n)) as s from remote('127.0.0.{1,2}', currentDatabase(), t_04625) group by s
format Null
settings enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0,
         max_bytes_before_external_group_by = 10000000, max_bytes_ratio_before_external_group_by = 0, max_threads = 1;

select toString(cityHash64(n)) as s from remote('127.0.0.{1,2}', currentDatabase(), t_04625) group by s
format Null
settings enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0,
         max_bytes_before_external_group_by = 10000000, max_bytes_ratio_before_external_group_by = 0, max_threads = 1,
         max_temporary_data_on_disk_size_for_query = 1000000; -- { serverError TOO_MANY_ROWS_OR_BYTES }

drop table t_04625;
