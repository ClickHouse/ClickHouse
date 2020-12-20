drop table if exists dist_01528;
create table dist_01528 as system.one engine=Distributed('test_cluster_two_shards', system, one, rand()+dummy);

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;
select * from dist_01528 where dummy = 2; -- { serverError 507; }
select * from dist_01528 where dummy = 2 settings allow_nondeterministic_optimize_skip_unused_shards=1;

drop table dist_01528;
