-- Tags: no-random-merge-tree-settings

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

drop table if exists t;
create table t (x UInt64) engine = MergeTree order by x;
insert into t select number from numbers_mt(10000000) settings max_insert_threads=8;

set allow_prefetched_read_pool_for_remote_filesystem = 0;
set allow_prefetched_read_pool_for_local_filesystem = 0;

-- { echo }

-- The number of output streams is limited by max_streams_for_merge_tree_reading
select sum(x) from t settings max_threads=32, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=0;
select * from (explain pipeline select sum(x) from t settings max_threads=32, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=0) where explain like '%Resize%' or explain like '%MergeTreeSelect%';

-- Without asynchronous_read, max_streams_for_merge_tree_reading limits max_streams * max_streams_to_max_threads_ratio
select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=0, max_streams_to_max_threads_ratio=8;
select * from (explain pipeline select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=0, max_streams_to_max_threads_ratio=8) where explain like '%Resize%' or explain like '%MergeTreeSelect%';

-- With asynchronous_read, read in max_streams_for_merge_tree_reading async streams and resize to max_threads
select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1;
select * from (explain pipeline select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1) where explain like '%Resize%' or explain like '%MergeTreeSelect%';

-- With asynchronous_read, read using max_streams * max_streams_to_max_threads_ratio async streams, resize to max_streams_for_merge_tree_reading outp[ut streams, resize to max_threads after aggregation
select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, max_streams_to_max_threads_ratio=8;
select * from (explain pipeline select sum(x) from t settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, max_streams_to_max_threads_ratio=8) where explain like '%Resize%' or explain like '%MergeTreeSelect%';

-- For read-in-order, disable everything
set query_plan_remove_redundant_sorting=0; -- to keep reading in order
select sum(x) from (select x from t order by x) settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, optimize_read_in_order=1, query_plan_read_in_order=1;
select * from (explain pipeline select sum(x) from (select x from t order by x) settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, optimize_read_in_order=1, query_plan_read_in_order=1) where explain like '%Resize%';
select sum(x) from (select x from t order by x) settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, max_streams_to_max_threads_ratio=8, optimize_read_in_order=1, query_plan_read_in_order=1;
select * from (explain pipeline select sum(x) from (select x from t order by x) settings max_threads=4, max_streams_for_merge_tree_reading=16, allow_asynchronous_read_from_io_pool_for_merge_tree=1, max_streams_to_max_threads_ratio=8, optimize_read_in_order=1, query_plan_read_in_order=1) where explain like '%Resize%';

-- { echoOff }
drop table t;
