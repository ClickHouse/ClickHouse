-- Tags: distributed

-- TODO: correct testing with real unique shards

set optimize_distributed_group_by_sharding_key=1;

-- Some queries in this test require sorting after aggregation.
set max_bytes_before_external_group_by = 0;

drop table if exists dist_01247;
drop table if exists data_01247;

create table data_01247 as system.numbers engine=Memory();
insert into data_01247 select * from system.numbers limit 2;
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, number);
-- since data is not inserted via distributed it will have duplicates
-- (and this is how we ensure that this optimization will work)

set max_distributed_connections=1;
set prefer_localhost_replica=0;
set enable_positional_arguments=0;

select '-';
select * from dist_01247;

select 'optimize_skip_unused_shards';
set optimize_skip_unused_shards=1;
select * from dist_01247;

select 'GROUP BY number';
select count(), * from dist_01247 group by number;
select 'GROUP BY number distributed_group_by_no_merge';
select count(), * from dist_01247 group by number settings distributed_group_by_no_merge=1;

-- dumb, but should work, since "GROUP BY 1" optimized out
select 'GROUP BY number, 1';
select count(), * from dist_01247 group by number, 1;
select 'GROUP BY 1';
select count(), min(number) from dist_01247 group by 1;

select 'GROUP BY number ORDER BY number DESC';
select count(), * from dist_01247 group by number order by number desc;

select 'GROUP BY toString(number)';
select count(), any(number) from dist_01247 group by toString(number);

select 'GROUP BY number%2';
select count(), any(number) from dist_01247 group by number%2;

select 'countDistinct';
select count(DISTINCT number) from dist_01247;

select 'countDistinct GROUP BY number';
select count(DISTINCT number) from dist_01247 group by number;

select 'DISTINCT';
select DISTINCT number from dist_01247;

select 'HAVING';
select count() cnt, * from dist_01247 group by number having cnt == 2;

select 'HAVING LIMIT';
select count() cnt, * from dist_01247 group by number having cnt == 1 limit 1;

select 'LIMIT';
select count(), * from dist_01247 group by number limit 1;
select 'LIMIT OFFSET';
select count(), * from dist_01247 group by number limit 1 offset 1;
select 'OFFSET distributed_push_down_limit=0';
select count(), * from dist_01247 group by number offset 1 settings distributed_push_down_limit=0;
select 'OFFSET distributed_push_down_limit=1';
select count(), * from dist_01247 group by number order by count(), number offset 1 settings distributed_push_down_limit=1;
-- this will emulate different data on for different shards
select 'WHERE LIMIT OFFSET';
select count(), * from dist_01247 where number = _shard_num-1 group by number order by number limit 1 offset 1;

select 'LIMIT BY 1';
select count(), * from dist_01247 group by number order by number limit 1 by number;

select 'GROUP BY (Distributed-over-Distributed)';
select count(), * from cluster(test_cluster_two_shards, currentDatabase(), dist_01247) group by number;
select 'GROUP BY (Distributed-over-Distributed) distributed_group_by_no_merge';
select count(), * from cluster(test_cluster_two_shards, currentDatabase(), dist_01247) group by number settings distributed_group_by_no_merge=1;

select 'GROUP BY (extemes)';
select count(), * from dist_01247 group by number settings extremes=1;

select 'LIMIT (extemes)';
select count(), * from dist_01247 group by number limit 1 settings extremes=1;

select 'GROUP BY WITH TOTALS';
select count(), * from dist_01247 group by number with totals;
select 'GROUP BY WITH ROLLUP';
select count(), * from dist_01247 group by number with rollup;
select 'GROUP BY WITH CUBE';
select count(), * from dist_01247 group by number with cube;

select 'GROUP BY WITH TOTALS ORDER BY';
select count(), * from dist_01247 group by number with totals order by number;

select 'GROUP BY WITH TOTALS ORDER BY LIMIT';
select count(), * from dist_01247 group by number with totals order by number limit 1;

select 'GROUP BY WITH TOTALS LIMIT';
select count(), * from dist_01247 group by number with totals limit 1;

-- GROUP BY (compound)
select 'GROUP BY (compound)';
drop table if exists dist_01247;
drop table if exists data_01247;
create table data_01247 engine=Memory() as select number key, 0 value from numbers(2);
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, key);
select 'GROUP BY sharding_key, ...';
select * from dist_01247 group by key, value;
select 'GROUP BY ..., sharding_key';
select * from dist_01247 group by value, key;

-- sharding_key (compound)
select 'sharding_key (compound)';
select k1, k2, sum(v) from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v), cityHash64(k1, k2)) group by k1, k2; -- optimization applied
select k1, any(k2), sum(v) from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v), cityHash64(k1, k2)) group by k1; -- optimization does not applied
select distinct k1, k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v), cityHash64(k1, k2)); -- optimization applied
select distinct on (k1) k2 from remote('127.{1,2}', view(select 1 k1, 2 k2, 3 v), cityHash64(k1, k2)); -- optimization does not applied

-- window functions
select 'window functions';
select key, sum(sum(value)) over (rows unbounded preceding) from dist_01247 group by key;

drop table dist_01247;
drop table data_01247;
