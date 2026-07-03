set enable_analyzer=1;
set session_timezone='UTC';

select '1970-01-01 00:00:01.000'::DateTime64(3) from remote('127.0.0.{1,2}', 'system.one');
select ['1970-01-01 00:00:01.000']::Array(DateTime64(3)) from remote('127.0.0.{1,2}', 'system.one');
select map('a', '1970-01-01 00:00:01.000')::Map(String, DateTime64(3)) from remote('127.0.0.{1,2}', 'system.one');
select tuple('1970-01-01 00:00:01.000')::Tuple(d DateTime64(3)) from remote('127.0.0.{1,2}', 'system.one');
select '1970-01-01 00:00:01.000'::Variant(DateTime64(3)) from remote('127.0.0.{1,2}', 'system.one');
select '1970-01-01 00:00:01.000'::DateTime64(3)::Dynamic from remote('127.0.0.{1,2}', 'system.one');
select '{"a" : "1970-01-01 00:00:01.000"}'::JSON(a DateTime64(3)) from remote('127.0.0.{1,2}', 'system.one');
select map('a', [tuple('1970-01-01 00:00:01.000')])::Map(String, Array(Tuple(d Variant(DateTime64(3))))) from remote('127.0.0.{1,2}', 'system.one');

select '1970-01-01'::Date32::Dynamic from remote('127.0.0.{1,2}', 'system.one');
select '1970-01-01'::Date::Dynamic from remote('127.0.0.{1,2}', 'system.one');
select '1970-01-01 00:00:01'::DateTime::Dynamic from remote('127.0.0.{1,2}', 'system.one');
select [tuple('1970-01-01')]::Array(Tuple(Date32))::Dynamic as d, dynamicType(d) from remote('127.0.0.{1,2}', 'system.one');

select [tuple('1970-01-01')]::Array(Tuple(Date))::Dynamic as d, dynamicType(d) from remote('127.0.0.{1,2}', 'system.one');
select [tuple('1970-01-01 00:00:01')]::Array(Tuple(DateTime))::Dynamic as d, dynamicType(d) from remote('127.0.0.{1,2}', 'system.one');
select [tuple('1970-01-01 00:00:01.00')]::Array(Tuple(DateTime64(3)))::Dynamic as d, dynamicType(d) from remote('127.0.0.{1,2}', 'system.one');

select '{"a" : 42, "b" : "1970-01-01", "c" : "1970-01-01 00:00:01", "d" : "1970-01-01 00:00:01.00"}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.{1,2}', 'system.one');
select map('a', ['{"a" : 42, "b" : "1970-01-01", "c" : "1970-01-01 00:00:01", "d" : "1970-01-01 00:00:01.00"}'])::Map(String, Array(Variant(JSON))) as json, JSONAllPathsWithTypes(assumeNotNull(variantElement(json['a'][1], 'JSON'))) from remote('127.0.0.{1,2}', 'system.one');
select '{"a" : [{"aa" : [42]}]}'::JSON as json, JSONAllPathsWithTypes(arrayJoin(json.a[])) from remote('127.0.0.{1,2}', 'system.one');
select '{"a" : [{"aa" : ["1970-01-01"]}]}'::JSON as json, JSONAllPathsWithTypes(arrayJoin(json.a[])) from remote('127.0.0.{1,2}', 'system.one');
select '{"a" : [{"aa" : ["1970-01-01 00:00:01"]}]}'::JSON as json, JSONAllPathsWithTypes(arrayJoin(json.a[])) from remote('127.0.0.{1,2}', 'system.one');
select '{"a" : [{"aa" : ["1970-01-01 00:00:01.000"]}]}'::JSON as json, JSONAllPathsWithTypes(arrayJoin(json.a[])) from remote('127.0.0.{1,2}', 'system.one');
