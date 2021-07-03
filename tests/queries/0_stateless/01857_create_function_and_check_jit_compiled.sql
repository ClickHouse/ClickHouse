create function MyFunc as (a, b, c) -> a + b > c AND c < 10;
select MyFunc(1, 2, 3);
select MyFunc(2, 2, 3);
select MyFunc(20, 20, 11);
drop function MyFunc;
