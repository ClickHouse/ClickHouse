set enable_positional_arguments = 1;

drop table if exists test;
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

select max(x3), max(x2), max(x1) from test group by 1; -- { serverError 43 }
select max(x1) from test order by 1; -- { serverError 43 }


