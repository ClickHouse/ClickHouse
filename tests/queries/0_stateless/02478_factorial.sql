select factorial(-1) = 1;
select factorial(0) = 1;
select factorial(10) = 3628800;

select factorial(100); -- { serverError 36 }
select factorial('100'); -- { serverError 43 }
select factorial(100.1234); -- { serverError 43 }
