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

-- A DateTime constant must cross the wire as an exact instant: 1698541800 and 1698538200 are two distinct
-- UTC instants that share the local text '2023-10-29 02:10:00' in Europe/Berlin. The lambda keeps the
-- expression on the shard; `toUnixTimestamp(<const>)` would fold on the initiator and assert nothing.
select arrayMap(x -> toUnixTimestamp(x), [toDateTime(1698541800, 'Europe/Berlin')]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x), [toDateTime(1698538200, 'Europe/Berlin')]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x.a), ['{"a":1698541800}'::JSON(a DateTime('Europe/Berlin'))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x.1), [tuple(toDateTime(1698541800, 'Europe/Berlin'), toDecimal64(1.5, 2))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
