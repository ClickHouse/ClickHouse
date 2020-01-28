set optimize_skip_unused_shards=1;

drop table if exists data_01068;
drop table if exists dist_01068;

create table data_01068 (key Int) Engine=Null();

create table dist_01068 as data_01068 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01068);
set force_optimize_skip_unused_shards=0;
select * from dist_01068;
set force_optimize_skip_unused_shards=1;
select * from dist_01068;
set force_optimize_skip_unused_shards=2;
select * from dist_01068; -- { serverError 507 }

drop table if exists dist_01068;
create table dist_01068 as data_01068 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01068, key%2);
set force_optimize_skip_unused_shards=0;
select * from dist_01068;
set force_optimize_skip_unused_shards=1;
select * from dist_01068; -- { serverError 507 }
set force_optimize_skip_unused_shards=2;
select * from dist_01068; -- { serverError 507 }

drop table if exists data_01068;
drop table if exists dist_01068;
