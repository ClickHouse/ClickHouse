drop table if exists test_smt;

create table test_smt (id UInt32, sMap SimpleAggregateFunction(sumMap, Tuple(Array(UInt8), Array(Int64))), aMap AggregateFunction(sumMap, Tuple(Array(UInt8), Array(Int64)))) engine SummingMergeTree partition by tuple() order by id;

insert into test_smt select id, sumMap(k), sumMapState(k) from (select 2 as id, arrayJoin([([0], [1]), ([0, 25], [-1, toInt64(1)])]) as k) group by id, rowNumberInAllBlocks();

select sumMap(sMap), sumMapMerge(aMap) from test_smt;

drop table if exists test_smt;
