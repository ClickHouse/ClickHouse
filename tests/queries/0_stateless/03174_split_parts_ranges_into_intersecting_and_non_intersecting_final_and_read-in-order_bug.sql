create table tab (x DateTime, y UInt32, v Int32) engine = ReplacingMergeTree(v) order by x;
insert into tab select toDateTime('2000-01-01') + number, number, 1 from numbers(1e7);
optimize table tab final;

select toStartOfDay(x) as k, sum(y) as v from tab final group by k order by k settings max_threads=8, optimize_aggregation_in_order=1, split_parts_ranges_into_intersecting_and_non_intersecting_final=1;
