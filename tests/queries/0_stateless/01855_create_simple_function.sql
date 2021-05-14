create function MyFunc as (a, b, c) -> a * b * c;
select MyFunc(2, 3, 4);
drop function MyFunc;
