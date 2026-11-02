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

-- A `DateTime` constant must cross the wire as an exact instant: 1698541800 and 1698538200 are two distinct
-- UTC instants that share the local text '2023-10-29 02:10:00' in Europe/Berlin. The lambda keeps the
-- expression on the shard; `toUnixTimestamp(<const>)` would fold on the initiator and assert nothing.
select arrayMap(x -> toUnixTimestamp(x), [toDateTime(1698541800, 'Europe/Berlin')]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x), [toDateTime(1698538200, 'Europe/Berlin')]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x.a), ['{"a":1698541800}'::JSON(a DateTime('Europe/Berlin'))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x.1), [tuple(toDateTime(1698541800, 'Europe/Berlin'), toDecimal64(1.5, 2))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
-- The three disjuncts become one `in` whose right operand is a bare tuple with no `_CAST` around it, so
-- the shard takes the set element type from the left side; `x in (<const>)` goes out the `_CAST` exit.
select arrayMap(x -> toDateTime(x, 'Europe/Berlin') = toDateTime(1698541800, 'Europe/Berlin') or toDateTime(x, 'Europe/Berlin') = toDateTime(1698000000, 'Europe/Berlin') or toDateTime(x, 'Europe/Berlin') = toDateTime(1699000000, 'Europe/Berlin'), [1698538200, 1698541800]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0, optimize_min_equality_disjunction_chain_length = 3;
select arrayMap(x -> (toUnixTimestamp(x.a), x.b), ['{"a":1698541800,"b":1.5}'::JSON(a DateTime('Europe/Berlin'), b Decimal64(2))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(variantElement(x, 'DateTime(\'Europe/Berlin\')')), [toDateTime(1698541800, 'Europe/Berlin')::Variant(DateTime('Europe/Berlin'), Decimal64(2))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;

-- The `in` right operand above is a bare tuple only while the disjunction chain is rewritten into one `in`,
-- so pin that `Tuple(DateTime(...))` constant: one disjunct short of the threshold the rewrite declines and
-- the three surviving `equals` render their constants through the cast path instead.
select count() > 0 from (explain query tree run_passes = 1 select arrayMap(x -> toDateTime(x, 'Europe/Berlin') = toDateTime(1698541800, 'Europe/Berlin') or toDateTime(x, 'Europe/Berlin') = toDateTime(1698000000, 'Europe/Berlin') or toDateTime(x, 'Europe/Berlin') = toDateTime(1699000000, 'Europe/Berlin'), [1698538200, 1698541800]) settings optimize_min_equality_disjunction_chain_length = 3) where explain ilike '%constant_value_type: Tuple(DateTime(%';
-- A `Map` reaches its date-times through the nested `Array(Tuple(K, V))`, and a `Nullable` leaf through its
-- nested column. A `Decimal64` value instead routes the whole map into exact serialization, where the key is
-- its own recursion; projecting the decimal keeps that arm honest about the value it travelled with.
select arrayMap(x -> toUnixTimestamp(x['k']), [map('k', toDateTime(1698541800, 'Europe/Berlin')::Nullable(DateTime('Europe/Berlin')))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> (toUnixTimestamp(mapKeys(x)[1]), mapValues(x)[1]), [map(toDateTime(1698541800, 'Europe/Berlin'), toDecimal64(1.5, 2))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
-- `Nullable` recurses into the type it wraps, so a `Decimal64` under it routes the whole value into exact
-- serialization, where the `Nullable` is its own recursion. A map value is a separate recursion from its
-- key, both in a `Map` and in a `Map` typed path of a `JSON`, which is rendered as a JSON object.
select arrayMap(x -> (toUnixTimestamp(x.1), x.2), [CAST(tuple(toDateTime(1698541800, 'Europe/Berlin'), toDecimal64(1.5, 2)) AS Nullable(Tuple(DateTime('Europe/Berlin'), Decimal64(2))))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0, enable_nullable_tuple_type = 1;
select arrayMap(x -> (toUnixTimestamp((x['k']).1), (x['k']).2), [map('k', tuple(toDateTime(1698541800, 'Europe/Berlin'), toDecimal64(1.5, 2)))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
select arrayMap(x -> toUnixTimestamp(x.m['k']), ['{"m":{"k":1698541800}}'::JSON(m Map(String, DateTime('Europe/Berlin')))]) from remote('127.0.0.1', 'system.one') settings prefer_localhost_replica = 0;
