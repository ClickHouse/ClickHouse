set optimize_skip_unused_shards=1;

drop table if exists data_02000;
drop table if exists dist_02000;

create table data_02000 (key Int) Engine=Null();
create table dist_02000 as data_02000 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_02000, key);

select * from data_02000 where key = 0xdeadbeafdeadbeaf;
select * from dist_02000 where key = 0xdeadbeafdeadbeaf settings force_optimize_skip_unused_shards=2; -- { serverError 507; }
select * from dist_02000 where key = 0xdeadbeafdeadbeaf;

drop table data_02000;
drop table dist_02000;
