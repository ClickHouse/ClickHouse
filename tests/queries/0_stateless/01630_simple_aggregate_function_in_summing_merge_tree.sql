drop table if exists test_smt;

create table test_smt (id UInt32, sMap SimpleAggregateFunction(sumMap, Tuple(Array(UInt8), Array(Int64))), aMap AggregateFunction(sumMap, Tuple(Array(UInt8), Array(Int64)))) engine SummingMergeTree partition by tuple() order by id;

insert into test_smt select id, sumMap(k), sumMapState(k) from (select 2 as id, arrayJoin([([0], [1]), ([0, 25], [-1, toInt64(1)])]) as k) group by id, rowNumberInAllBlocks();

select sumMap(sMap), sumMapMerge(aMap) from test_smt;

drop table if exists test_smt;

drop table if exists simple_agf_summing_mt;

create table simple_agf_summing_mt (a Int64, grp_aggreg AggregateFunction(groupUniqArrayArray, Array(UInt64)), grp_simple SimpleAggregateFunction(groupUniqArrayArray, Array(UInt64))) engine = SummingMergeTree() order by a;

insert into simple_agf_summing_mt select 1 a, groupUniqArrayArrayState([toUInt64(number)]), groupUniqArrayArray([toUInt64(number)]) from numbers(1) group by a;

insert into simple_agf_summing_mt select 1 a, groupUniqArrayArrayState([toUInt64(number)]), groupUniqArrayArray([toUInt64(number)]) from numbers(2) group by a;

optimize table simple_agf_summing_mt final;

SELECT arraySort(groupUniqArrayArrayMerge(grp_aggreg)) gra , arraySort(groupUniqArrayArray(grp_simple)) grs FROM simple_agf_summing_mt group by a;

drop table if exists simple_agf_summing_mt;
