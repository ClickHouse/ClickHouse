-- Tags: no-random-merge-tree-settings, no-parallel-replicas

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

drop table if exists t;
create table t (x UInt64) engine = MergeTree order by x;
insert into t select number from numbers_mt(10000000) settings max_insert_threads=8;

set allow_prefetched_read_pool_for_remote_filesystem = 0;
set allow_prefetched_read_pool_for_local_filesystem = 0;
-- Pin to prevent two-level merge from changing pipeline structure when parts > threshold
-- (randomized 0-100, INSERT with max_insert_threads=8 creates ~8 parts)
set read_in_order_two_level_merge_threshold = 100;

-- The pipeline shape depends on the exact thread count;
-- disable the free-memory limiter to keep `max_threads` as set in the queries.
-- Without this, on `per_test_coverage` and other memory-constrained builds
-- `max_threads` is clamped to fewer threads and e.g. `Resize 16 → 4` becomes
-- `Resize 16 → 3`. See PR #100383.
set max_threads_min_free_memory_per_thread = 0;
-- The volume-based stream cap is a second, unrelated limiter on the stream counts this
-- test asserts: at 16Mi it caps `MergeTreeSelect x 32` down to the 16-stream floor.
-- 0 disables it, matching the uncapped shape the reference was recorded with.
set merge_tree_min_bytes_per_read_stream = 0;
set max_insert_threads_min_free_memory_per_thread = 0;

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

-- With asynchronous_read, read using max_streams * max_streams_to_max_threads_ratio async streams, resize to max_streams_for_merge_tree_reading output streams; the aggregation without keys then collapses the output to a single stream
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
