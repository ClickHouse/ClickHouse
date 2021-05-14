create function MyFunc3 as (a, b) -> a + b;
create function MyFunc3 as (a) -> a || '!!!'; -- { serverError 585 }
create function cast as x -> x + 1; -- { serverError 585 }
drop function MyFunc3;
