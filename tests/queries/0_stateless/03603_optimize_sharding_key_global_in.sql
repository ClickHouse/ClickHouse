set enable_analyzer = 1,
    optimize_skip_unused_shards = 1,
    prefer_localhost_replica = 0,
    optimize_sharding_key_global_in = 1;

drop table if exists 03603_local;
create table 03603_local (key UInt32, val String) engine = MergeTree order by key
as
select number, 'val-' || number from numbers(1000);

select '-- Simple shard key';

drop table if exists 03603_dist_simple;

create table 03603_dist_simple (key UInt32, val String)
engine = Distributed(test_cluster_two_shards, currentDatabase(), 03603_local, key);

select '';
select * from 03603_dist_simple where key global in (select 1 as f union all select 500) order by key;

select '';
select count() from 03603_dist_simple where key global in (select -1 as f union all select -500) order by 1;

select '';
select * from 03603_dist_simple where key + 1 global in (select 1 as f union all select 500) order by key;

drop table 03603_dist_simple;

select '';
select '-- Hash shard key';

drop table if exists 03603_dist_hash;

create table 03603_dist_hash (key UInt32, val String)
engine = Distributed(test_cluster_two_shards, currentDatabase(), 03603_local, xxHash32(key));

select '';
select * from 03603_dist_hash where key global in (select 1 as f union all select 500) order by key;

select '';
select * from 03603_dist_hash where key + 1 global in (select 1 as f union all select 500) order by key;

drop table 03603_dist_hash;

select '';
select '-- Complex shard key';

drop table if exists 03603_dist_complex;

create table 03603_dist_complex (key UInt32, val String)
engine = Distributed(test_cluster_two_shards, currentDatabase(), 03603_local, intDiv(xxHash32(key + 1), 100));

select '';
select * from 03603_dist_complex where key global in (select 1 as f union all select 500) order by key;

select '';
select * from 03603_dist_complex where key + 1 global in (select 1 as f union all select 500) order by key;

drop table 03603_dist_complex;

select '';
select '-- No shard key';

drop table if exists 03603_dist_nokey;

create table 03603_dist_nokey (key UInt32, val String)
engine = Distributed(test_cluster_two_shards, currentDatabase(), 03603_local);

select '';
select * from 03603_dist_nokey where key global in (select 1 as f union all select 500) order by key;

drop table 03603_dist_nokey;

drop table 03603_local;

select '';

system flush logs query_log;

with [
    currentDatabase() || '.`03603_dist_simple`',
    currentDatabase() || '.`03603_dist_hash`',
    currentDatabase() || '.`03603_dist_complex`',
    currentDatabase() || '.`03603_dist_nokey`'
] as dist_tables,
queries as (
    select query_id, event_time_microseconds, arrayIntersect(tables, dist_tables)[1] as dist_table_used
    from system.query_log
    where current_database = currentDatabase()
    and type = 'QueryFinish'
    and query_kind = 'Select'
    and is_initial_query = 1
    and hasAny(tables, dist_tables)
)
select queries.dist_table_used,
       replaceRegexpOne(extract(query_log.query, 'GLOBAL IN (\(.+\)) ORDER BY'), '\_data\_[0-9\_]+', '_data_xxx')
from system.query_log
    join queries on query_log.initial_query_id = queries.query_id
where type = 'QueryFinish'
  and is_initial_query = 0
  and initial_query_id in (select query_id from queries)
order by queries.event_time_microseconds, query_log.query;
