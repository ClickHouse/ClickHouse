set optimize_skip_unused_shards=1;

drop table if exists data_01071;
drop table if exists dist_01071;

create table data_01071 (key Int) Engine=Null();

create table dist_01071 as data_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01071);
set force_optimize_skip_unused_shards=0;
select * from dist_01071;
set force_optimize_skip_unused_shards=1;
select * from dist_01071;
set force_optimize_skip_unused_shards=2;
select * from dist_01071; -- { serverError 507 }

drop table if exists dist_01071;
create table dist_01071 as data_01071 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01071, key%2);
set force_optimize_skip_unused_shards=0;
select * from dist_01071;
set force_optimize_skip_unused_shards=1;
select * from dist_01071; -- { serverError 507 }
set force_optimize_skip_unused_shards=2;
select * from dist_01071; -- { serverError 507 }

drop table if exists data_01071;
drop table if exists dist_01071;
