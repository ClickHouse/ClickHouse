create function MyFunc3 as (a, b) -> a + b;
create function MyFunc3 as (a) -> a || '!!!'; -- { serverError 587 }
create function cast as x -> x + 1; -- { serverError 587 }
drop function MyFunc3;
