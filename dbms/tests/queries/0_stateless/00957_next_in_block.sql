-- no arguments
select nextInBlock(); -- { serverError 42 }
-- greater than 3 arguments
select nextInBlock(1,2,3,4); -- { serverError 42 }
-- zero offset value
select nextInBlock(dummy, 0); -- { serverError 69 }
-- negative offset value
select nextInBlock(dummy, -1); -- { serverError 43 }
-- non-constant offset value
select nextInBlock(dummy, dummy); -- { serverError 43 }
-- bad default value
select nextInBlock(dummy, 1, 'hello'); -- { serverError 43 }
-- single argument test
select number, nextInBlock(number) from numbers(2);
-- filling by column's default value
select number, nextInBlock(number, 2) from numbers(3);
-- offset is greater that block - should fill everything with defaults
select number, nextInBlock(number, 5) from numbers(2);
-- substitution by constant for missing values
select number, nextInBlock(number, 2, 1000) from numbers(5);
-- substitution by expression
-- select number, nextInBlock(number, 2, number % 2) from numbers(5);