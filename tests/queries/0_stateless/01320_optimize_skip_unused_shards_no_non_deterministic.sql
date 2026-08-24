-- Tags: shard

drop table if exists data_01320;
drop table if exists dist_01320;

create table data_01320 (key Int) Engine=Null();
-- non deterministic function (i.e. rand())
create table dist_01320 as data_01320 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01320, key + rand());

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;
select * from dist_01320 where key = 0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

drop table data_01320;
drop table dist_01320;
