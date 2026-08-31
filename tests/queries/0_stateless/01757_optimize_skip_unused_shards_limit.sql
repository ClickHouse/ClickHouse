-- Tags: shard

drop table if exists dist_01757;
create table dist_01757 as system.one engine=Distributed(test_cluster_two_shards, system, one, dummy);

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;

-- in
select * from dist_01757 where dummy in (0,) format Null;
select * from dist_01757 where dummy in (0, 1) format Null settings optimize_skip_unused_shards_limit=2;

-- in negative
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=1; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

-- or negative
select * from dist_01757 where dummy = 0 or dummy = 1 settings optimize_skip_unused_shards_limit=1; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

-- or
select * from dist_01757 where dummy = 0 or dummy = 1 format Null settings optimize_skip_unused_shards_limit=2;

-- and negative
-- disabled for analyzer cause new implementation consider `dummy = 0 and dummy = 1` as constant False.
select * from dist_01757 where dummy = 0 and dummy = 1 settings optimize_skip_unused_shards_limit=1, enable_analyzer=0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
select * from dist_01757 where dummy = 0 and dummy = 2 and dummy = 3 settings optimize_skip_unused_shards_limit=1, enable_analyzer=0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
select * from dist_01757 where dummy = 0 and dummy = 2 and dummy = 3 settings optimize_skip_unused_shards_limit=2, enable_analyzer=0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

-- and
select * from dist_01757 where dummy = 0 and dummy = 1 settings optimize_skip_unused_shards_limit=2;
select * from dist_01757 where dummy = 0 and dummy = 1 and dummy = 3 settings optimize_skip_unused_shards_limit=3;

-- ARGUMENT_OUT_OF_BOUND error
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=0; -- { serverError ARGUMENT_OUT_OF_BOUND }
select * from dist_01757 where dummy in (0, 1) settings optimize_skip_unused_shards_limit=9223372036854775808; -- { serverError ARGUMENT_OUT_OF_BOUND }

drop table dist_01757;

-- fuzzed
SELECT * FROM remote('127.0.0.{1,2}', numbers(40), number) ORDER BY 'a' LIMIT 1 BY number SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards=0 format Null
