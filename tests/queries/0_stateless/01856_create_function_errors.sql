create function MyFunc2 as (a, b) -> a || b || c; -- { serverError 47 }

create function MyFunc2 as (a, b) -> MyFunc2(a, b) + MyFunc2(a, b); -- { serverError 593 } recursive function

create function cast as a -> a + 1; -- { serverError 591 } function already exist

create function sum as (a, b) -> a + b; -- { serverError 591 } aggregate function already exist

create function MyFunc3 as (a, b) -> a + b;

create function MyFunc3 as (a) -> a || '!!!'; -- { serverError 591 } function already exist

drop function MyFunc3;
