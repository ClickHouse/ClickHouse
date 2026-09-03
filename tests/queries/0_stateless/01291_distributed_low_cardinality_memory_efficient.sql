-- Tags: distributed

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS dist;

create table data (key String) Engine=Memory();
create table dist (key LowCardinality(String)) engine=Distributed(test_cluster_two_shards, currentDatabase(), data);
insert into data values ('foo');

set distributed_aggregation_memory_efficient=1;

-- There is an obscure bug in rare corner case.
set max_bytes_before_external_group_by = 0;
set max_bytes_ratio_before_external_group_by = 0;

select * from dist group by key;

DROP TABLE data;
DROP TABLE dist;
