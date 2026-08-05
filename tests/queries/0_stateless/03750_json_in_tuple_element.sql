set enable_analyzer=1;

select tupleElement('{"a" : 42}'::JSON, 'a');
select tupleElement('{"a" : 42}'::JSON(a UInt32), 'a');
select tupleElement(materialize('{"a" : 42}')::JSON, 'a');
select tupleElement(materialize('{"a" : 42}')::JSON(a UInt32), 'a');

select '{"a" : 42}'::JSON.a;
select '{"a" : 42}'::JSON(a UInt32).a;
select materialize('{"a" : 42}')::JSON.a;
select materialize('{"a" : 42}')::JSON(a UInt32).a;


select ['{"a" : 42}']::Array(JSON)[1].a;
select ['{"a" : 42}']::Array(JSON(a UInt32))[1].a;
select materialize(['{"a" : 42}'])::Array(JSON)[1].a;
select materialize(['{"a" : 42}'])::Array(JSON(a UInt32))[1].a;


drop table if exists test;
create table test (json JSON(a UInt32)) engine=Memory;
insert into test values ('{"a" : 42, "b" : "s1", "c" : [{"d" : 1}, {"d" : 2}, {"d" : 3}]}');
select tupleElement(json, 'a') from test;
select tupleElement(json, 'b') from test;
select tupleElement(json, 'c') from test;
select tupleElement(json, 'c.:`Array(JSON)`') from test;
select json.c[1].d from test;

explain syntax run_query_tree_passes=1 select tupleElement(json, 'a') from test settings optimize_functions_to_subcolumns=1;

drop table test;
