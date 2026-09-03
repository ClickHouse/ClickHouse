select '{"a.b.c" : 42}'::JSON.a.b.c;
select materialize('{"a.b.c" : 42}')::JSON.a.b.c;
select '{"a.b" : [{"c.d" : 42}]}'::JSON.a.b[1].c.d;
select materialize('{"a.b" : [{"c.d" : 42}]}')::JSON.a.b[1].c.d;

select '{"a.b.c" : 42}'::JSON as json, dynamicType(tupleElement(json, 'a'));
select '{"a.b.c" : 42}'::JSON as json, dynamicType(tupleElement(json, 'a.b'));
select '{"a.b.c" : 42}'::JSON as json, dynamicType(tupleElement(json, 'a.b.c'));

select arrayJoin(['{"a" : 42}', '{"a" : {"b" : 42}}'])::JSON as json, dynamicType(tupleElement(json, 'a'));
