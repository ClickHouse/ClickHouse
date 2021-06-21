create function MyFunc3 as (a, b) -> a + b;
create function MyFunc3 as (a) -> a || '!!!'; -- { serverError 588 }
create function cast as x -> x + 1; -- { serverError 588 }
drop function MyFunc3;
