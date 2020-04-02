-- no arguments
select neighbor(); -- { serverError 42 }
-- single argument
select neighbor(1); -- { serverError 42 }
-- greater than 3 arguments
select neighbor(1,2,3,4); -- { serverError 42 }
-- bad default value
select neighbor(dummy, 1, 'hello'); -- { serverError 386 }
-- types without common supertype (UInt64 and Int8)
select number, neighbor(number, 1, -10) from numbers(3); -- { serverError 386 }
-- nullable offset is not allowed
select number, if(number > 1, number, null) as offset, neighbor(number, offset) from numbers(3); -- { serverError 43 }
select 'Zero offset';
select number, neighbor(number, 0) from numbers(3);
select 'Nullable values';
select  if(number > 1, number, null) as value, number as offset, neighbor(value, offset) as neighbor from numbers(3);
select 'Result with different type';
select toInt32(number) as n, neighbor(n, 1, -10) from numbers(3);
select 'Offset > block';
select number, neighbor(number, 10) from numbers(3);
select 'Abs(Offset) > block';
select number, neighbor(number, -10) from numbers(3);
select 'Positive offset';
select number, neighbor(number, 1) from numbers(3);
select 'Negative offset';
select number, neighbor(number, 1) from numbers(3);
select 'Positive offset with defaults';
select number, neighbor(number, 2, number + 10) from numbers(4);
select 'Negative offset with defaults';
select number, neighbor(number, -2, number + 10) from numbers(4);
select 'Positive offset with const defaults';
select number, neighbor(number, 1, 1000) from numbers(3);
select 'Negative offset with const defaults';
select number, neighbor(number, -1, 1000) from numbers(3);
select 'Dynamic column and offset, out of bounds';
select number, number * 2 as offset, neighbor(number, offset, number * 10) from numbers(4);
select 'Dynamic column and offset, negative';
select number, -number * 2 as offset, neighbor(number, offset, number * 10) from numbers(6);
select 'Dynamic column and offset, without defaults';
select number, -(number - 2) * 2 as offset, neighbor(number, offset) from numbers(6);
select 'Constant column';
select number, neighbor(1000, 10) from numbers(3);