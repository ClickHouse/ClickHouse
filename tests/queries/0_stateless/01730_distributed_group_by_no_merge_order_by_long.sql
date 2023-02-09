-- Tags: long, distributed

drop table if exists data_01730;

-- does not use 127.1 due to prefer_localhost_replica

select * from remote('127.{2..11}', view(select * from numbers(1e6))) group by number order by number limit 20 settings distributed_group_by_no_merge=0, max_memory_usage='100Mi'; -- { serverError 241 }
-- no memory limit error, because with distributed_group_by_no_merge=2 remote servers will do ORDER BY and will cut to the LIMIT
select * from remote('127.{2..11}', view(select * from numbers(1e6))) group by number order by number limit 20 settings distributed_group_by_no_merge=2, max_memory_usage='100Mi';

-- since the MergingSortedTransform will start processing only when all ports (remotes) will have some data,
-- and the query with GROUP BY on remote servers will first do GROUP BY and then send the block,
-- so the initiator will first receive all blocks from remotes and only after start merging,
-- and will hit the memory limit.
select * from remote('127.{2..11}', view(select * from numbers(1e6))) group by number order by number limit 1e6 settings distributed_group_by_no_merge=2, max_memory_usage='100Mi'; -- { serverError 241 }

-- with optimize_aggregation_in_order=1 remote servers will produce blocks more frequently,
-- since they don't need to wait until the aggregation will be finished,
-- and so the query will not hit the memory limit error.
create table data_01730 engine=MergeTree() order by key as select number key from numbers(1e6);
select * from remote('127.{2..11}', currentDatabase(), data_01730) group by key order by key limit 1e6 settings distributed_group_by_no_merge=2, max_memory_usage='100Mi', optimize_aggregation_in_order=1 format Null;
drop table data_01730;
