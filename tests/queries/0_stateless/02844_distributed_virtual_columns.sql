drop table if exists data_01072;
drop table if exists dist_01072;
create table data_01072 (key Int) Engine=MergeTree() ORDER BY key;
create table dist_01072 (key Int) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key);
select * from dist_01072 where key=0 and _part='0';
