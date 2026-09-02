-- TODO: set special_sort = 'opencl_bitonic';

select toUInt8(number * 2) as x from numbers(42) order by x desc;
select toInt8(number * 2) as x from numbers(42) order by x desc;
select toUInt16(number * 2) as x from numbers(42) order by x desc;
select toInt16(number * 2) as x from numbers(42) order by x desc;
select toUInt32(number * 2) as x from numbers(42) order by x desc;
select toInt32(number * 2) as x from numbers(42) order by x desc;
select toUInt64(number * 2) as x from numbers(42) order by x desc;
select toInt64(number * 2) as x from numbers(42) order by x desc;
