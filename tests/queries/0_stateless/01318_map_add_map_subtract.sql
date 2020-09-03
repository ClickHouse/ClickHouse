drop table if exists map_test;
create table map_test engine=TinyLog() as (select ([1, number], [toInt32(2),2]) as map from numbers(1, 10));

-- mapAdd
select mapAdd([1], [1]); -- { serverError 42 }
select mapAdd(([1], [1])); -- { serverError 42 }
select mapAdd(([1], [1]), map) from map_test; -- { serverError 43 }
select mapAdd(([toUInt64(1)], [1]), map) from map_test; -- { serverError 43 }
select mapAdd(([toUInt64(1), 2], [toInt32(1)]), map) from map_test; -- {serverError 42 }

select mapAdd(([toUInt64(1)], [toInt32(1)]), map) from map_test;
select mapAdd(cast(map, 'Tuple(Array(UInt8), Array(UInt8))'), ([1], [1]), ([2],[2]) ) from map_test;

-- cleanup
drop table map_test;

-- check types
select mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res);
select mapAdd(([toUInt16(1), 2], [toUInt16(1), 1]), ([toUInt16(1), 2], [toUInt16(1), 1])) as res, toTypeName(res);
select mapAdd(([toUInt32(1), 2], [toUInt32(1), 1]), ([toUInt32(1), 2], [toUInt32(1), 1])) as res, toTypeName(res);
select mapAdd(([toUInt64(1), 2], [toUInt64(1), 1]), ([toUInt64(1), 2], [toUInt64(1), 1])) as res, toTypeName(res);

select mapAdd(([toInt8(1), 2], [toInt8(1), 1]), ([toInt8(1), 2], [toInt8(1), 1])) as res, toTypeName(res);
select mapAdd(([toInt16(1), 2], [toInt16(1), 1]), ([toInt16(1), 2], [toInt16(1), 1])) as res, toTypeName(res);
select mapAdd(([toInt32(1), 2], [toInt32(1), 1]), ([toInt32(1), 2], [toInt32(1), 1])) as res, toTypeName(res);
select mapAdd(([toInt64(1), 2], [toInt64(1), 1]), ([toInt64(1), 2], [toInt64(1), 1])) as res, toTypeName(res);

select mapAdd(([1, 2], [toFloat32(1.1), 1]), ([1, 2], [2.2, 1])) as res, toTypeName(res);
select mapAdd(([1, 2], [toFloat64(1.1), 1]), ([1, 2], [2.2, 1])) as res, toTypeName(res);
select mapAdd(([toFloat32(1), 2], [toFloat64(1.1), 1]), ([toFloat32(1), 2], [2.2, 1])) as res, toTypeName(res); -- { serverError 44 }
select mapAdd(([1, 2], [toFloat64(1.1), 1]), ([1, 2], [1, 1])) as res, toTypeName(res); -- { serverError 43 }
select mapAdd((['a', 'b'], [1, 1]), ([key], [1])) from values('key String', ('b'), ('c'), ('d'));
select mapAdd((cast(['a', 'b'], 'Array(FixedString(1))'), [1, 1]), ([key], [1])) as res, toTypeName(res) from values('key FixedString(1)', ('b'), ('c'), ('d'));
select mapAdd((cast(['a', 'b'], 'Array(LowCardinality(String))'), [1, 1]), ([key], [1])) from values('key String', ('b'), ('c'), ('d'));
select mapAdd((key, val), (key, val)) as res, toTypeName(res) from values ('key Array(Enum16(\'a\'=1, \'b\'=2)), val Array(Int16)',  (['a'], [1]), (['b'], [1]));
select mapAdd((key, val), (key, val)) as res, toTypeName(res) from values ('key Array(Enum8(\'a\'=1, \'b\'=2)), val Array(Int16)',  (['a'], [1]), (['b'], [1]));
select mapAdd((key, val), (key, val)) as res, toTypeName(res) from values ('key Array(UUID), val Array(Int32)', (['00000000-89ab-cdef-0123-456789abcdef'], [1]), (['11111111-89ab-cdef-0123-456789abcdef'], [2]));

-- mapSubtract, same rules as mapAdd
select mapSubtract(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res);
select mapSubtract(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [2, 2])) as res, toTypeName(res); -- overflow
select mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt16(2), 2])) as res, toTypeName(res);
select mapSubtract(([1, 2], [toFloat32(1.1), 1]), ([1, 2], [2.2, 1])) as res, toTypeName(res);
select mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt16(2), 2])) as res, toTypeName(res);
select mapSubtract(([toUInt8(3)], [toInt32(1)]), ([toUInt8(1), 2], [toInt32(2), 2])) as res, toTypeName(res);
