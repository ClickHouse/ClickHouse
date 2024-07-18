drop table if exists mapop_test;
set allow_experimental_map_type = 1;
create table mapop_test engine=TinyLog() as (select map(1, toInt32(2), number, 2) as m from numbers(1, 10));

-- mapAdd
select mapAdd(map(1, 1)); -- { serverError 42 }
select mapAdd(map(1, 1), m) from mapop_test; -- { serverError 43 }

select mapAdd(map(toUInt64(1), toInt32(1)), m) from mapop_test;
select mapAdd(cast(m, 'Map(UInt8, UInt8)'), map(1, 1), map(2,2)) from mapop_test;

-- cleanup
drop table mapop_test;

-- check types
select mapAdd(map(toUInt8(1), 1, 2, 1), map(toUInt8(1), 1, 2, 1)) as res, toTypeName(res);
select mapAdd(map(toUInt16(1), toUInt16(1), 2, 1), map(toUInt16(1), toUInt16(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toUInt32(1), toUInt32(1), 2, 1), map(toUInt32(1), toUInt32(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toUInt64(1), toUInt64(1), 2, 1), map(toUInt64(1), toUInt64(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toUInt128(1), toUInt128(1), 2, 1), map(toUInt128(1), toUInt128(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toUInt256(1), toUInt256(1), 2, 1), map(toUInt256(1), toUInt256(1), 2, 1)) as res, toTypeName(res);

select mapAdd(map(toInt8(1), 1, 2, 1), map(toInt8(1), 1, 2, 1)) as res, toTypeName(res);
select mapAdd(map(toInt16(1), toInt16(1), 2, 1), map(toInt16(1), toInt16(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toInt32(1), toInt32(1), 2, 1), map(toInt32(1), toInt32(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toInt64(1), toInt64(1), 2, 1), map(toInt64(1), toInt64(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toInt128(1), toInt128(1), 2, 1), map(toInt128(1), toInt128(1), 2, 1)) as res, toTypeName(res);
select mapAdd(map(toInt256(1), toInt256(1), 2, 1), map(toInt256(1), toInt256(1), 2, 1)) as res, toTypeName(res);

select mapAdd(map(1, toFloat32(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, toTypeName(res);
select mapAdd(map(1, toFloat64(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, toTypeName(res);
select mapAdd(map(1, toFloat64(1.1), 2, 1), map(1, 1, 2, 1)) as res, toTypeName(res); -- { serverError 43 }
select mapAdd(map('a', 1, 'b', 1), map(key, 1)) from values('key String', ('b'), ('c'), ('d'));
select mapAdd(map(cast('a', 'FixedString(1)'), 1, 'b', 1), map(key, 1)) as res, toTypeName(res) from values('key String', ('b'), ('c'), ('d'));
select mapAdd(map(cast('a', 'LowCardinality(String)'), 1, 'b', 1), map(key, 1)) from values('key String', ('b'), ('c'), ('d'));
select mapAdd(map(key, val), map(key, val)) as res, toTypeName(res) from values ('key Enum16(\'a\'=1, \'b\'=2), val Int16',  ('a', 1), ('b', 1));
select mapAdd(map(key, val), map(key, val)) as res, toTypeName(res) from values ('key Enum8(\'a\'=1, \'b\'=2), val Int16',  ('a', 1), ('b', 1));
select mapAdd(map(key, val), map(key, val)) as res, toTypeName(res) from values ('key UUID, val Int32', ('00000000-89ab-cdef-0123-456789abcdef', 1), ('11111111-89ab-cdef-0123-456789abcdef', 2));

-- mapSubtract, same rules as mapAdd
select mapSubtract(map(toUInt8(1), 1, 2, 1), map(toUInt8(1), 1, 2, 1)) as res, toTypeName(res);
select mapSubtract(map(toUInt8(1), 1, 2, 1), map(toUInt8(1), 2, 2, 2)) as res, toTypeName(res); -- overflow
select mapSubtract(map(toUInt8(1), toInt32(1), 2, 1), map(toUInt8(1), toInt16(2), 2, 2)) as res, toTypeName(res);
select mapSubtract(map(1, toFloat32(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, toTypeName(res);
select mapSubtract(map(toUInt8(1), toInt32(1), 2, 1), map(toUInt8(1), toInt16(2), 2, 2)) as res, toTypeName(res);
select mapSubtract(map(toUInt8(3), toInt32(1)), map(toUInt8(1), toInt32(2), 2, 2)) as res, toTypeName(res);
