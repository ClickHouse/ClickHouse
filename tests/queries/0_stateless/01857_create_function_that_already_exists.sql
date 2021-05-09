create function MyFunc3 as (a, b) -> a + b;
create function MyFunc3 as (a) -> a || '!!!'; -- { serverError 49 }
