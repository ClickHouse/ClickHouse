drop table if exists summing_merge_tree_aggregate_function;
drop table if exists summing_merge_tree_null;

---- partition merge
set allow_deprecated_syntax_for_merge_tree=1;
create table summing_merge_tree_aggregate_function (
    d Date,
    k UInt64,
    u AggregateFunction(uniq, UInt64)
) engine=SummingMergeTree(d, k, 1);

insert into summing_merge_tree_aggregate_function
select today() as d,
       number as k,
       uniqState(toUInt64(number % 500))
from numbers(5000)
group by d, k;

insert into summing_merge_tree_aggregate_function
select today() as d,
       number + 5000 as k,
       uniqState(toUInt64(number % 500))
from numbers(5000)
group by d, k;

select count() from summing_merge_tree_aggregate_function;
optimize table summing_merge_tree_aggregate_function;
select count() from summing_merge_tree_aggregate_function;

drop table summing_merge_tree_aggregate_function;

---- sum + uniq + uniqExact
set allow_deprecated_syntax_for_merge_tree=1;
create table summing_merge_tree_aggregate_function (
    d materialized today(),
    k UInt64,
    c UInt64,
    u AggregateFunction(uniq, UInt8),
    ue AggregateFunction(uniqExact, UInt8)
) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(1), uniqExactState(1);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(2), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(3), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(1), uniqExactState(1);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(2), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(3), uniqExactState(3);

select
    k, sum(c),
    uniqMerge(u), uniqExactMerge(ue)
from summing_merge_tree_aggregate_function group by k;

optimize table summing_merge_tree_aggregate_function;

select
    k, sum(c),
    uniqMerge(u), uniqExactMerge(ue)
from summing_merge_tree_aggregate_function group by k;

drop table summing_merge_tree_aggregate_function;

---- sum + topK
create table summing_merge_tree_aggregate_function (d materialized today(), k UInt64, c UInt64, x AggregateFunction(topK(2), UInt8)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
select k, sum(c), topKMerge(2)(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), topKMerge(2)(x) from summing_merge_tree_aggregate_function group by k;

drop table summing_merge_tree_aggregate_function;

---- sum + topKWeighted
create table summing_merge_tree_aggregate_function (d materialized today(), k UInt64, c UInt64, x AggregateFunction(topKWeighted(2), UInt8, UInt8)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(2, 2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(2, 2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(3, 5);
select k, sum(c), topKWeightedMerge(2)(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), topKWeightedMerge(2)(x) from summing_merge_tree_aggregate_function group by k;

drop table summing_merge_tree_aggregate_function;

---- avg
create table summing_merge_tree_aggregate_function (d materialized today(), k UInt64, x AggregateFunction(avg, Float64)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, avgState(0.0);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.125);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.25);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.375);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.4375);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.5);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.5625);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.625);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.75);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.875);
insert into summing_merge_tree_aggregate_function select 1, avgState(1.0);
select k, avgMerge(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, avgMerge(x) from summing_merge_tree_aggregate_function group by k;

drop table summing_merge_tree_aggregate_function;

---- quantile
create table summing_merge_tree_aggregate_function (d materialized today(), k UInt64, x AggregateFunction(quantile(0.1), Float64)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.0);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.1);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.2);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.3);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.4);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.5);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.6);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.7);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.8);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.9);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(1.0);
select k, round(quantileMerge(0.1)(x), 1) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, round(quantileMerge(0.1)(x), 1) from summing_merge_tree_aggregate_function group by k;

drop table summing_merge_tree_aggregate_function;

---- sum + uniq with more data
create table summing_merge_tree_null (
    d materialized today(),
    k UInt64,
    c UInt64,
    u UInt64
) engine=Null;

create materialized view summing_merge_tree_aggregate_function (
    d Date,
    k UInt64,
    c UInt64,
    u AggregateFunction(uniq, UInt64)
) engine=SummingMergeTree(d, k, 8192)
as select d, k, sum(c) as c, uniqState(u) as u
from summing_merge_tree_null
group by d, k;

-- prime number 53 to avoid resonanse between %3 and %53
insert into summing_merge_tree_null select number % 3, 1, number % 53 from numbers(999999);

select k, sum(c), uniqMerge(u) from summing_merge_tree_aggregate_function group by k order by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), uniqMerge(u) from summing_merge_tree_aggregate_function group by k order by k;

drop table summing_merge_tree_aggregate_function;
drop table summing_merge_tree_null;
