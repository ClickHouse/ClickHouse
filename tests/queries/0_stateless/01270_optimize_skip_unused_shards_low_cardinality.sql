-- Tags: shard

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;
set allow_suspicious_low_cardinality_types=1;

drop table if exists data_01270;
drop table if exists dist_01270;

create table data_01270 (key LowCardinality(Int)) Engine=Null();
create table dist_01270 as data_01270 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01270, key);
select * from dist_01270 where key = 1;

drop table data_01270;
drop table dist_01270;
