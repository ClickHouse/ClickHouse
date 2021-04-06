SET enable_global_with_statement = 1;

with 1 as x select x;
with 1 as x select * from (select x);
with 1 as x select *, x from (with 2 as x select x as y);
with 1 as x select x union all select x;
select x from (with 1 as x select x union all with 2 as x select x) order by x;
with 5 as q1, x as (select number+100 as b, number as a from numbers(10) where number > q1) select * from x;

explain syntax with 1 as x select x;
explain syntax with 1 as x select * from (select x);
explain syntax with 1 as x select *, x from (with 2 as x select x as y);
explain syntax with 1 as x select x union all select x;
explain syntax with 1 as x select x union all with 2 as x select x;
explain syntax with 5 as q1, x as (select number + 100 as b, number as a from numbers(10) where number > q1) select * from x;
