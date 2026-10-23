-- Tags: long, no-tsan, no-asan, no-msan, no-fasttest
-- Test is slow
-- Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(229376, None); merge_max_block_size=(8192, None)
-- A small granule size multiplies the mark count of this 1e7-row FINAL merge by orders of magnitude,
-- which can push it past the client's receive timeout. merge_max_block_size is bounded as a precaution.
create table tab (x DateTime('UTC'), y UInt32, v Int32) engine = ReplacingMergeTree(v) order by x;
insert into tab select toDateTime('2000-01-01', 'UTC') + number, number, 1 from numbers(1e7);
optimize table tab final;

WITH (60 * 60) * 24 AS d
select toStartOfDay(x) as k, sum(y) as v,
  (z + d) * (z + d - 1) / 2 - (toUInt64(k - toDateTime('2000-01-01', 'UTC')) as z) * (z - 1) / 2 as est,
  est - v as delta
from tab final group by k order by k
settings max_threads=8, optimize_aggregation_in_order=1, split_parts_ranges_into_intersecting_and_non_intersecting_final=1;
