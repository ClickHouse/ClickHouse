create function MyFunc2 as (a, b) -> a || b || c; -- { serverError 47 }
