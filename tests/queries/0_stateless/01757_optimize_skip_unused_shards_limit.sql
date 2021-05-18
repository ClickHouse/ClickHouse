drop table if exists dist_01757;
create table dist_01757 as system.one engine=Distributed(test_cluster_two_shards, system, one, dummy);

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;

-- in
select * from dist_01757 where dummy in (0,) format Null;
select * from dist_01757 where dummy in (0, 1) format Null settings optimize_skip_unused_shards_limit=2;

-- in negative
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=1; -- { serverError 507 }

-- or negative
select * from dist_01757 where dummy = 0 or dummy = 1 settings optimize_skip_unused_shards_limit=1; -- { serverError 507 }

-- or
select * from dist_01757 where dummy = 0 or dummy = 1 format Null settings optimize_skip_unused_shards_limit=2;

-- and negative
select * from dist_01757 where dummy = 0 and dummy = 1 settings optimize_skip_unused_shards_limit=1; -- { serverError 507 }
select * from dist_01757 where dummy = 0 and dummy = 2 and dummy = 3 settings optimize_skip_unused_shards_limit=1; -- { serverError 507 }
select * from dist_01757 where dummy = 0 and dummy = 2 and dummy = 3 settings optimize_skip_unused_shards_limit=2; -- { serverError 507 }

-- and
select * from dist_01757 where dummy = 0 and dummy = 1 settings optimize_skip_unused_shards_limit=2;
select * from dist_01757 where dummy = 0 and dummy = 1 and dummy = 3 settings optimize_skip_unused_shards_limit=3;

-- ARGUMENT_OUT_OF_BOUND error
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=0; -- { serverError 69 }
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=9223372036854775808; -- { serverError 69 }

drop table dist_01757;
