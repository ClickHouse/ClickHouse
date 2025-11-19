set allow_experimental_parallel_reading_from_replicas = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

drop table if exists 03581_data;

create table 03581_data (
    key UInt32,

    val_minmax UInt32,
    val_set UInt32,

    index skip_minmax val_minmax type set(0) granularity 1,
    index skip_set val_set type set(0) granularity 1,
)
engine = MergeTree
order by key
settings index_granularity = 10;

insert into 03581_data select number, number, number from numbers(1000);

select 'Primary key:', count() from 03581_data where key = 2000;
select 'Skip index MinMax:', count() from 03581_data where val_minmax = 2000;
select 'Skip index Set:', count() from 03581_data where val_set = 2000;

select '';
select 'Rows read:';

system flush logs query_log;

select read_rows
from system.query_log
where current_database = currentDatabase()
  and type = 'QueryFinish'
  and query ilike '% from 03581_data where %'
order by event_time_microseconds desc;

drop table 03581_data;
