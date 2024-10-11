create table t(a UInt32, b UInt32) engine=MergeTree order by (a, b) settings index_granularity=1;

system stop merges t;

-- for this part the first columns is useless, so we have to use both
insert into t select 42, number from numbers_mt(100);

-- for this part the first columns is enough
insert into t select number, number from numbers_mt(100);

-- force reloading index
detach table t;
attach table t;

set merge_tree_min_bytes_for_concurrent_read=1, merge_tree_min_rows_for_concurrent_read=1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=1.0, max_threads=4;

-- the bug happened when we used (a, b) index values for one part and only (a) for another in PartsSplitter. even a simple count query is enough,
-- because some granules were assinged to wrong layers and hence not returned from the reading step (because they were filtered out by `FilterSortedStreamByRange`)
select count() from t where not ignore(*);
