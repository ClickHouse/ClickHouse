create table test (json JSON) engine=MergeTree order by tuple();
insert into test select '{"a" : "str"}';

-- This won't work, see https://github.com/ClickHouse/ClickHouse/issues/89854
-- select count(), toString(json.a) from test group by toString(json.a) settings enable_analyzer=0, optimize_injective_functions_in_group_by=0;

select count(), toString(json.a) from test group by toString(json.a) settings enable_analyzer=1, optimize_injective_functions_in_group_by=0;
select count(), toString(json.a) from test group by toString(json.a) settings enable_analyzer=1, optimize_injective_functions_in_group_by=1;
explain query tree select count(), toString(json.a) from test group by toString(json.a) settings enable_analyzer=1, optimize_injective_functions_in_group_by=1;
drop table test;

