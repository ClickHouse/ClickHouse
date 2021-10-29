set enable_positional_arguments = 1;

drop table if exists test;
drop table if exists test2;

create table test(x1 Int, x2 Int, x3 Int) engine=Memory();
insert into test values (1, 10, 100), (10, 1, 10), (100, 100, 1);

-- { echo }
select x3, x2, x1 from test order by 1;
select x3, x2, x1 from test order by x3;

select x3, x2, x1 from test order by 1 desc;
select x3, x2, x1 from test order by x3 desc;

insert into test values (1, 10, 200), (10, 1, 200), (100, 100, 1);
select x3, x2 from test group by x3, x2;
select x3, x2 from test group by 1, 2;

select x1, x2, x3 from test order by x3 limit 1 by x3;
select x1, x2, x3 from test order by 3 limit 1 by 3;
select x1, x2, x3 from test order by x3 limit 1 by x1;
select x1, x2, x3 from test order by 3 limit 1 by 1;

explain syntax select x3, x2, x1 from test order by 1 + 1;
explain syntax select x3, x2, x1 from test order by (1 + 1) * 3;

select x2, x1 from test group by x2 + x1; -- { serverError 215 }
select x2, x1 from test group by 1 + 2; -- { serverError 215 }

explain syntax select x3, x2, x1 from test order by 1;
explain syntax select x3 + 1, x2, x1 from test order by 1;
explain syntax select x3, x3 - x2, x2, x1 from test order by 2;
explain syntax select x3, if(x3 > 10, x3, plus(x1, x2)), x1 + x2 from test order by 2;
explain syntax select max(x1), x2 from test group by 2 order by 1, 2;
explain syntax select 1 + greatest(x1, 1), x2 from test group by 1, 2;

select max(x1), x2 from test group by 1, 2; -- { serverError 43 }
select 1 + max(x1), x2 from test group by 1, 2; -- { serverError 43 }
select x1 + x2, x3 from test group by x1 + x2, x3;

select x3, x2, x1 from test order by x3 * 2, x2, x1; -- check x3 * 2 does not become x3 * x2

explain syntax select x1, x3 from test group by 1 + 2, 1, 2;
explain syntax select x1 + x3, x3 from test group by 1, 2;

create table test2(x1 Int, x2 Int, x3 Int) engine=Memory;
insert into test2 values (1, 10, 100), (10, 1, 10), (100, 100, 1);
select x1, x1 * 2, max(x2), max(x3) from test2 group by 2, 1, x1 order by 1, 2, 4 desc, 3 asc;
