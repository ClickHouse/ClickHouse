-- no arguments
select neighbour(); -- { serverError 42 }
-- single argument
select neighbour(1); -- { serverError 42 }
-- greater than 3 arguments
select neighbour(1,2,3,4); -- { serverError 42 }
-- bad default value
select neighbour(dummy, 1, 'hello'); -- { serverError 43 }
-- types without common supertype (UInt64 and Int8)
select number, neighbour(number, 1, -10) from numbers(3); -- { serverError 43 }
-- nullable offset is not allowed
select number, if(number > 1, number, null) as offset, neighbour(number, offset) from numbers(3); -- { serverError 43 }
select 'Zero offset';
select number, neighbour(number, 0) from numbers(3);
select 'Nullable values';
select  if(number > 1, number, null) as value, number as offset, neighbour(value, offset) as neighbour from numbers(3);
select 'Result with different type';
select toInt32(number) as n, neighbour(n, 1, -10) from numbers(3);
select 'Offset > block';
select number, neighbour(number, 10) from numbers(3);
select 'Abs(Offset) > block';
select number, neighbour(number, -10) from numbers(3);
select 'Positive offset';
select number, neighbour(number, 1) from numbers(3);
select 'Negative offset';
select number, neighbour(number, 1) from numbers(3);
select 'Positive offset with defaults';
select number, neighbour(number, 2, number + 10) from numbers(4);
select 'Negative offset with defaults';
select number, neighbour(number, -2, number + 10) from numbers(4);
select 'Positive offset with const defaults';
select number, neighbour(number, 1, 1000) from numbers(3);
select 'Negative offset with const defaults';
select number, neighbour(number, -1, 1000) from numbers(3);
select 'Dynamic column and offset, out of bounds';
select number, number * 2 as offset, neighbour(number, offset, number * 10) from numbers(4);
select 'Dynamic column and offset, negative';
select number, -number * 2 as offset, neighbour(number, offset, number * 10) from numbers(6);
select 'Dynamic column and offset, without defaults';
select number, -(number - 2) * 2 as offset, neighbour(number, offset) from numbers(6);
select 'Constant column';
select number, neighbour(1000, 10) from numbers(3);