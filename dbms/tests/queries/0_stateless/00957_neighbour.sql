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
select 'Constant column';
select number, neighbour(1000, 10) from numbers(3);