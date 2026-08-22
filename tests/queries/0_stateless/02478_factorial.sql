select factorial(-1) = 1;
select factorial(0) = 1;
select factorial(10) = 3628800;

select factorial(100); -- { serverError BAD_ARGUMENTS }
select factorial('100'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select factorial(100.1234); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
