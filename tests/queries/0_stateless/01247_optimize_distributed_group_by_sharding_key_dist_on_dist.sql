-- Tags: distributed

-- TODO: correct testing with real unique shards

set optimize_distributed_group_by_sharding_key=1;

drop table if exists dist_01247;
drop table if exists dist_layer_01247;
drop table if exists data_01247;

create table data_01247 as system.numbers engine=Memory();
-- since data is not inserted via distributed it will have duplicates
-- (and this is how we ensure that this optimization will work)
insert into data_01247 select * from system.numbers limit 2;

set max_distributed_connections=1;
set optimize_skip_unused_shards=1;

select 'Distributed(number)-over-Distributed(number)';
create table dist_layer_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, number);
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01247, number);
select count(), * from dist_01247 group by number order by number limit 1 settings prefer_localhost_replica=1;
select '-';
-- Now, sharding key optimization is not supported for distributed over distributed with serialized plan.
select count(), * from dist_01247 group by number order by number limit 1 settings prefer_localhost_replica=0, serialize_query_plan=1, enable_analyzer=1;
drop table if exists dist_01247;
drop table if exists dist_layer_01247;

select 'Distributed(rand)-over-Distributed(number)';
create table dist_layer_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, number);
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01247, rand());
select count(), * from dist_01247 group by number order by number limit 1;
drop table if exists dist_01247;
drop table if exists dist_layer_01247;

select 'Distributed(rand)-over-Distributed(rand)';
create table dist_layer_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, rand());
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01247, number);
select count(), * from dist_01247 group by number order by number limit 1;

drop table dist_01247;
drop table dist_layer_01247;
drop table data_01247;
