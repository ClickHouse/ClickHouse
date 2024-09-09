-- Tags: shard

set optimize_skip_unused_shards=1;

drop table if exists data_01071;
drop table if exists dist_01071;
drop table if exists data2_01071;
drop table if exists dist2_01071;
drop table if exists dist2_layer_01071;

create table data_01071 (key Int) Engine=Null();

create table dist_01071 as data_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01071);
set force_optimize_skip_unused_shards=0;
select * from dist_01071;
set force_optimize_skip_unused_shards=1;
select * from dist_01071;
set force_optimize_skip_unused_shards=2;
select * from dist_01071; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

drop table if exists dist_01071;
create table dist_01071 as data_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01071, key%2);
set force_optimize_skip_unused_shards=0;
select * from dist_01071;
set force_optimize_skip_unused_shards=1;
select * from dist_01071; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
set force_optimize_skip_unused_shards=2;
select * from dist_01071; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
drop table if exists dist_01071;

-- non deterministic function (i.e. rand())
create table dist_01071 as data_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01071, key + rand());
set force_optimize_skip_unused_shards=1;
select * from dist_01071 where key = 0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

drop table if exists data_01071;
drop table if exists dist_01071;

-- Distributed on Distributed
set distributed_group_by_no_merge=1;
set force_optimize_skip_unused_shards=2;
create table data2_01071 (key Int, sub_key Int) Engine=Null();
create table dist2_layer_01071 as data2_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data2_01071, sub_key%2);
create table dist2_01071 as data2_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), dist2_layer_01071, key%2);
select * from dist2_01071 where key = 1; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
set force_optimize_skip_unused_shards_nesting=1;
select * from dist2_01071 where key = 1;
drop table if exists data2_01071;
drop table if exists dist2_layer_01071;
drop table if exists dist2_01071;
