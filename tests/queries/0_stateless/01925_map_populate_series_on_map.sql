-- { echo }
drop table if exists map_test;
set allow_experimental_map_type = 1;
create table map_test engine=TinyLog() as (select (number + 1) as n, map(1, 1, number,2) as m from numbers(1, 5));

select mapPopulateSeries(m) from map_test;
select mapPopulateSeries(m, toUInt64(3)) from map_test;
select mapPopulateSeries(m, toUInt64(10)) from map_test;
select mapPopulateSeries(m, 1000) from map_test; -- { serverError 43 }
select mapPopulateSeries(m, n) from map_test;

drop table map_test;

select mapPopulateSeries(map(toUInt8(1), toUInt8(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toUInt16(1), toUInt16(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toUInt32(1), toUInt32(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toUInt64(1), toUInt64(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toUInt128(1), toUInt128(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toUInt256(1), toUInt256(1), 2, 1)) as res, toTypeName(res);

select mapPopulateSeries(map(toInt8(1), toInt8(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt16(1), toInt16(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt32(1), toInt32(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt64(1), toInt64(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt128(1), toInt128(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt256(1), toInt256(1), 2, 1)) as res, toTypeName(res);

select mapPopulateSeries(map(toInt8(-10), toInt8(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt16(-10), toInt16(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt32(-10), toInt32(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt64(-10), toInt64(1), 2, 1)) as res, toTypeName(res);
select mapPopulateSeries(map(toInt64(-10), toInt64(1), 2, 1), toInt64(-5)) as res, toTypeName(res);

select mapPopulateSeries(); -- { serverError 42 }
select mapPopulateSeries('asdf'); -- { serverError 43 }
select mapPopulateSeries(map('1', 1, '2', 1)) as res, toTypeName(res); -- { serverError 43 }
