SET enable_analyzer=1;

select d.String from (select 'str'::Dynamic as d);
select json.a from (select '{"a" : 42}'::JSON as json);
select json.a from (select '{"a" : 42}'::JSON(a UInt32) as json);
select json.a.:Int64 from (select materialize('{"a" : 42}')::JSON as json);
