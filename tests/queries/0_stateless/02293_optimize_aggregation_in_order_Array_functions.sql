drop table if exists data_02293;
create table data_02293 (a Int64, grp_aggreg AggregateFunction(groupArrayArray, Array(UInt64)), grp_simple SimpleAggregateFunction(groupArrayArray, Array(UInt64))) engine = MergeTree() order by a;
insert into data_02293 select 1 a, groupArrayArrayState([toUInt64(number)]), groupArrayArray([toUInt64(number)]) from numbers(2) group by a;
SELECT arraySort(groupArrayArrayMerge(grp_aggreg)) gra , arraySort(groupArrayArray(grp_simple)) grs FROM data_02293 group by a SETTINGS optimize_aggregation_in_order=1;
drop table data_02293;
