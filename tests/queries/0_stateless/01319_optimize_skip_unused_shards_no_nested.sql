drop table if exists data_01319;
drop table if exists dist_01319;
drop table if exists dist_layer_01319;

create table data_01319 (key Int, sub_key Int) Engine=Null();

set force_optimize_skip_unused_shards=2;
set optimize_skip_unused_shards=1;

create table dist_layer_01319 as data_01319 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01319, sub_key%2);
create table dist_01319 as data_01319 Engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01319, key%2);
select * from dist_01319 where key = 1; -- { serverError 507 }
set optimize_skip_unused_shards=2; -- no nested
select * from dist_01319 where key = 1;
