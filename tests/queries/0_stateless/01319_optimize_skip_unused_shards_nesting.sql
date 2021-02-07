drop table if exists data_01319;
drop table if exists dist_01319;
drop table if exists dist_layer_01319;

create table data_01319 (key Int, sub_key Int) Engine=Null();

create table dist_layer_01319 as data_01319 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01319, sub_key);
-- test_unavailable_shard here to check that optimize_skip_unused_shards always
-- remove some nodes from the cluster for the first nesting level
create table dist_01319 as data_01319 Engine=Distributed(test_unavailable_shard, currentDatabase(), dist_layer_01319, key+1);

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;

set force_optimize_skip_unused_shards_nesting=2;
set optimize_skip_unused_shards_nesting=2;
select * from dist_01319 where key = 1; -- { serverError 507 }
set force_optimize_skip_unused_shards_nesting=1;
select * from dist_01319 where key = 1;
set force_optimize_skip_unused_shards_nesting=2;
set optimize_skip_unused_shards_nesting=1;
select * from dist_01319 where key = 1;

drop table data_01319;
drop table dist_01319;
drop table dist_layer_01319;
