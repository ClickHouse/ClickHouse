create function MyFunc as (a, b, c) -> a * b * c;
select MyFunc(2, 3, 4);
select isConstant(MyFunc(1, 2, 3));
drop function MyFunc;
