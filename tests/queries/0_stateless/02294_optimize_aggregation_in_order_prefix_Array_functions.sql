drop table if exists data_02294;
create table data_02294 (a Int64, b Int64, grp_aggreg AggregateFunction(groupArrayArray, Array(UInt64)), grp_simple SimpleAggregateFunction(groupArrayArray, Array(UInt64))) engine = MergeTree() order by a;
insert into data_02294 select intDiv(number, 2) a, 0 b, groupArrayArrayState([toUInt64(number)]), groupArrayArray([toUInt64(number)]) from numbers(4) group by a, b;
SELECT arraySort(groupArrayArrayMerge(grp_aggreg)) gra , arraySort(groupArrayArray(grp_simple)) grs FROM data_02294 group by a, b SETTINGS optimize_aggregation_in_order=1;
drop table data_02294;
