create function MyFunc2 as (a, b) -> a || b || c;
select MyFunc2('1', '2'); -- { serverError 47 }
