-- no arguments
select neighbour(); -- { serverError 42 }
-- single argument
select neighbour(1); -- { serverError 42 }
-- greater than 3 arguments
select neighbour(1,2,3,4); -- { serverError 42 }
-- bad default value
select neighbour(dummy, 1, 'hello'); -- { serverError 43 }
-- single argument test
select number, neighbour(number,1) from numbers(2);
-- filling by column's default value
select number, neighbour(number, 2) from numbers(3);
-- offset is greater that block - should fill everything with defaults
select number, neighbour(number, 5) from numbers(2);
-- substitution by constant for missing values
select number, neighbour(number, 2, 1000) from numbers(5);
-- substitution by expression
-- select number, neighbour(number, 2, number % 2) from numbers(5);