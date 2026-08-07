select * from (select 0 as k, toInt8(1) as v) t1 asof join (select 0 as k, toInt8(0) as v) t2 using(k, v);
select * from (select 0 as k, toInt16(1) as v) t1 asof join (select 0 as k, toInt16(0) as v) t2 using(k, v);
select * from (select 0 as k, toInt32(1) as v) t1 asof join (select 0 as k, toInt32(0) as v) t2 using(k, v);
select * from (select 0 as k, toInt64(1) as v) t1 asof join (select 0 as k, toInt64(0) as v) t2 using(k, v);

select * from (select 0 as k, toUInt8(1) as v) t1 asof join (select 0 as k, toUInt8(0) as v) t2 using(k, v);
select * from (select 0 as k, toUInt16(1) as v) t1 asof join (select 0 as k, toUInt16(0) as v) t2 using(k, v);
select * from (select 0 as k, toUInt32(1) as v) t1 asof join (select 0 as k, toUInt32(0) as v) t2 using(k, v);
select * from (select 0 as k, toUInt64(1) as v) t1 asof join (select 0 as k, toUInt64(0) as v) t2 using(k, v);

select * from (select 0 as k, toDecimal32(1, 0) as v) t1 asof join (select 0 as k, toDecimal32(0, 0) as v) t2 using(k, v);
select * from (select 0 as k, toDecimal64(1, 0) as v) t1 asof join (select 0 as k, toDecimal64(0, 0) as v) t2 using(k, v);
select * from (select 0 as k, toDecimal128(1, 0) as v) t1 asof join (select 0 as k, toDecimal128(0, 0) as v) t2 using(k, v);

select * from (select 0 as k, toDate(0) as v) t1 asof join (select 0 as k, toDate(0) as v) t2 using(k, v);
select * from (select 0 as k, toDateTime(0, 'UTC') as v) t1 asof join (select 0 as k, toDateTime(0, 'UTC') as v) t2 using(k, v);

select * from (select 0 as k, 'x' as v) t1 asof join (select 0 as k, 'x' as v) t2 using(k, v); -- { serverError BAD_TYPE_OF_FIELD }
