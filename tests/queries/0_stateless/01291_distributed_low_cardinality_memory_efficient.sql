-- Tags: distributed

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS dist;

create table data (key String) Engine=Memory();
create table dist (key LowCardinality(String)) engine=Distributed(test_cluster_two_shards, currentDatabase(), data);
insert into data values ('foo');
set distributed_aggregation_memory_efficient=1;
select * from dist group by key;

DROP TABLE data;
DROP TABLE dist;
