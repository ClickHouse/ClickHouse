select (1 ? ('abc' as s) : 'def') = s;
select (1 ? toFixedString('abc' as s, 3) : 'def') = s;
select (1 ? toFixedString('abc' as s, 3) : toFixedString('def', 3)) = s;
select (1 ? ('abc' as s) : toFixedString('def', 3)) = s;

select (1 ? (today() as t) : yesterday()) = t;

select (1 ? (now() as n) : now() - 1) = n;

select (1 ? (toUInt8(0) as i) : toUInt8(1)) = i;
select (1 ? (toUInt16(0) as i) : toUInt8(1)) = i;
select (1 ? (toUInt32(0) as i) : toUInt8(1)) = i;
select (1 ? (toUInt64(0) as i) : toUInt8(1)) = i;
select (1 ? (toInt8(0) as i) : toUInt8(1)) = i;
select (1 ? (toInt16(0) as i) : toUInt8(1)) = i;
select (1 ? (toInt32(0) as i) : toUInt8(1)) = i;
select (1 ? (toInt64(0) as i) : toUInt8(1)) = i;

select (1 ? (toUInt8(0) as i) : toUInt16(1)) = i;
select (1 ? (toUInt16(0) as i) : toUInt16(1)) = i;
select (1 ? (toUInt32(0) as i) : toUInt16(1)) = i;
select (1 ? (toUInt64(0) as i) : toUInt16(1)) = i;
select (1 ? (toInt8(0) as i) : toUInt16(1)) = i;
select (1 ? (toInt16(0) as i) : toUInt16(1)) = i;
select (1 ? (toInt32(0) as i) : toUInt16(1)) = i;
select (1 ? (toInt64(0) as i) : toUInt16(1)) = i;

select (1 ? (toUInt8(0) as i) : toUInt32(1)) = i;
select (1 ? (toUInt16(0) as i) : toUInt32(1)) = i;
select (1 ? (toUInt32(0) as i) : toUInt32(1)) = i;
select (1 ? (toUInt64(0) as i) : toUInt32(1)) = i;
select (1 ? (toInt8(0) as i) : toUInt32(1)) = i;
select (1 ? (toInt16(0) as i) : toUInt32(1)) = i;
select (1 ? (toInt32(0) as i) : toUInt32(1)) = i;
select (1 ? (toInt64(0) as i) : toUInt32(1)) = i;

select (1 ? (toUInt8(0) as i) : toUInt64(1)) = i;
select (1 ? (toUInt16(0) as i) : toUInt64(1)) = i;
select (1 ? (toUInt32(0) as i) : toUInt64(1)) = i;
select (1 ? (toUInt64(0) as i) : toUInt64(1)) = i;
--select (1 ? (toInt8(0) as i) : toUInt64(1)) = i;
--select (1 ? (toInt16(0) as i) : toUInt64(1)) = i;
--select (1 ? (toInt32(0) as i) : toUInt64(1)) = i;
--select (1 ? (toInt64(0) as i) : toUInt64(1)) = i;

select (1 ? (toUInt8(0) as i) : toInt8(1)) = i;
select (1 ? (toUInt16(0) as i) : toInt8(1)) = i;
select (1 ? (toUInt32(0) as i) : toInt8(1)) = i;
--select (1 ? (toUInt64(0) as i) : toInt8(1)) = i;
select (1 ? (toInt8(0) as i) : toInt8(1)) = i;
select (1 ? (toInt16(0) as i) : toInt8(1)) = i;
select (1 ? (toInt32(0) as i) : toInt8(1)) = i;
select (1 ? (toInt64(0) as i) : toInt8(1)) = i;

select (1 ? (toUInt8(0) as i) : toInt16(1)) = i;
select (1 ? (toUInt16(0) as i) : toInt16(1)) = i;
select (1 ? (toUInt32(0) as i) : toInt16(1)) = i;
--select (1 ? (toUInt64(0) as i) : toInt16(1)) = i;
select (1 ? (toInt8(0) as i) : toInt16(1)) = i;
select (1 ? (toInt16(0) as i) : toInt16(1)) = i;
select (1 ? (toInt32(0) as i) : toInt16(1)) = i;
select (1 ? (toInt64(0) as i) : toInt16(1)) = i;

select (1 ? (toUInt8(0) as i) : toInt32(1)) = i;
select (1 ? (toUInt16(0) as i) : toInt32(1)) = i;
select (1 ? (toUInt32(0) as i) : toInt32(1)) = i;
--select (1 ? (toUInt64(0) as i) : toInt32(1)) = i;
select (1 ? (toInt8(0) as i) : toInt32(1)) = i;
select (1 ? (toInt16(0) as i) : toInt32(1)) = i;
select (1 ? (toInt32(0) as i) : toInt32(1)) = i;
select (1 ? (toInt64(0) as i) : toInt32(1)) = i;

select (1 ? (toUInt8(0) as i) : toInt64(1)) = i;
select (1 ? (toUInt16(0) as i) : toInt64(1)) = i;
select (1 ? (toUInt32(0) as i) : toInt64(1)) = i;
--select (1 ? (toUInt64(0) as i) : toInt64(1)) = i;
select (1 ? (toInt8(0) as i) : toInt64(1)) = i;
select (1 ? (toInt16(0) as i) : toInt64(1)) = i;
select (1 ? (toInt32(0) as i) : toInt64(1)) = i;
select (1 ? (toInt64(0) as i) : toInt64(1)) = i;
