set enable_analyzer = 1;

select '{"a" : {"b" : null}}'::JSON(a.b Nullable(UInt32)) as json, json.^a, json.@a, tupleElement(json, 'a');
drop table if exists test;
create table test (json JSON(a.b Nullable(UInt32))) engine=MergeTree order by tuple() settings optimize_row_order_if_no_order_by = 0;
insert into test values ('{"a" : {"b" : null}}'), ('{"a" : {"b" : 42}}'), ('{}');
select json, json.^a, json.@a from test;
drop table test;

