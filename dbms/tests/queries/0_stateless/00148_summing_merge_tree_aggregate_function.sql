drop table if exists test.summing_merge_tree_aggregate_function;

create table test.summing_merge_tree_aggregate_function (d materialized today(), k UInt64, c UInt64, u AggregateFunction(uniq, UInt64)) engine=SummingMergeTree(d, k, 8192);

insert into test.summing_merge_tree_aggregate_function select 1, 1, uniqState(toUInt64(123));
insert into test.summing_merge_tree_aggregate_function select 1, 1, uniqState(toUInt64(456));
optimize table test.summing_merge_tree_aggregate_function;
select k, sum(c), uniqMerge(u) from test.summing_merge_tree_aggregate_function group by k;

drop table test.summing_merge_tree_aggregate_function;
