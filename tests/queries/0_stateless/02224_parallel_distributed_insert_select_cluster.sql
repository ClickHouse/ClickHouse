drop table if exists dst_02224;
drop table if exists src_02224;
create table dst_02224 (key Int) engine=Memory();
create table src_02224 (key Int) engine=Memory();
insert into src_02224 values (1);

-- { echoOn }
truncate table dst_02224;
insert into function cluster('test_cluster_two_shards', currentDatabase(), dst_02224, key)
select * from cluster('test_cluster_two_shards', currentDatabase(), src_02224, key)
settings parallel_distributed_insert_select=1, max_distributed_depth=1; -- { serverError TOO_LARGE_DISTRIBUTED_DEPTH }
select * from dst_02224;

truncate table dst_02224;
insert into function cluster('test_cluster_two_shards', currentDatabase(), dst_02224, key)
select * from cluster('test_cluster_two_shards', currentDatabase(), src_02224, key)
settings parallel_distributed_insert_select=1, max_distributed_depth=2;
select * from dst_02224;

truncate table dst_02224;
insert into function cluster('test_cluster_two_shards', currentDatabase(), dst_02224, key)
select * from cluster('test_cluster_two_shards', currentDatabase(), src_02224, key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02224;

truncate table dst_02224;
insert into function remote('127.{1,2}', currentDatabase(), dst_02224, key)
select * from remote('127.{1,2}', currentDatabase(), src_02224, key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02224;
-- { echoOff }

drop table src_02224;
drop table dst_02224;
