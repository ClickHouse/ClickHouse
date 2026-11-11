set group_by_two_level_threshold = 100000;
set enable_positional_arguments = 1;
set enable_analyzer = 1;

drop table if exists test;
drop table if exists test2;

create table test(x1 Int, x2 Int, x3 Int) engine=Memory();
insert into test values (1, 10, 100), (10, 1, 10), (100, 100, 1);

-- { echo }
select x3, x2, x1 from test order by 1;
select x3, x2, x1 from test order by -3;
select x3, x2, x1 from test order by x3;

select x3, x2, x1 from test order by 3;
select x3, x2, x1 from test order by -1;
select x3, x2, x1 from test order by x1;

select x3, x2, x1 from test order by 1 desc;
select x3, x2, x1 from test order by -3 desc;
select x3, x2, x1 from test order by x3 desc;

select x3, x2, x1 from test order by 3 desc;
select x3, x2, x1 from test order by -1 desc;
select x3, x2, x1 from test order by x1 desc;

insert into test values (1, 10, 100), (10, 1, 10), (100, 100, 1);
select x3, x2 from test group by x3, x2 order by x3;
select x3, x2 from test group by 1, 2 order by x3;

select x1, x2, x3 from test order by x3 limit 1 by x3;
select x1, x2, x3 from test order by 3 limit 1 by 3;
select x1, x2, x3 from test order by x3 limit 1 by x1;
select x1, x2, x3 from test order by 3 limit 1 by 1;

explain syntax select x3, x2, x1 from test order by 1;
explain syntax select x3 + 1, x2, x1 from test order by 1;
explain syntax select x3, x2, x1 from test order by -1;
explain syntax select x3 + 1, x2, x1 from test order by -1;
explain syntax select x3, x3 - x2, x2, x1 from test order by 2;
explain syntax select x3, x3 - x2, x2, x1 from test order by -2;
explain syntax select x3, if(x3 > 10, x3, plus(x1, x2)), x1 + x2 from test order by 2;
explain syntax select x3, if(x3 > 10, x3, plus(x1, x2)), x1 + x2 from test order by -2;
explain syntax select max(x1), x2 from test group by 2 order by 1, 2;
explain syntax select max(x1), x2 from test group by -1 order by -2, -1;
explain syntax select 1 + greatest(x1, 1), x2 from test group by 1, 2;
explain syntax select 1 + greatest(x1, 1), x2 from test group by -2, -1;

select max(x1), x2 from test group by 1, 2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, 184 }
select 1 + max(x1), x2 from test group by 1, 2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, 184 }
select max(x1), x2 from test group by -2, -1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, 184 }
select 1 + max(x1), x2 from test group by -2, -1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, 184 }

explain syntax select x1 + x3, x3 from test group by 1, 2;
explain syntax select x1 + x3, x3 from test group by -2, -1;

create table test2(x1 Int, x2 Int, x3 Int) engine=Memory;
insert into test2 values (1, 10, 100), (10, 1, 10), (100, 100, 1);
select x1, x1 * 2, max(x2), max(x3) from test2 group by 2, 1, x1 order by 1, 2, 4 desc, 3 asc;
select x1, x1 * 2, max(x2), max(x3) from test2 group by 2, 1, x1 order by 1, 2, -1 desc, -2 asc;

select a, b, c, d, e, f  from (select 44 a, 88 b, 13 c, 14 d, 15 e, 16 f) t group by 1,2,3,4,5,6 order by a;
select a, b, c, d, e, f  from (select 44 a, 88 b, 13 c, 14 d, 15 e, 16 f) t group by 1,2,3,-3,-2,-1 order by a;

explain syntax select plus(1, 1) as a group by a;
select substr('aaaaaaaaaaaaaa', 8) as a  group by a order by a;
select substr('aaaaaaaaaaaaaa', 8) as a  group by substr('aaaaaaaaaaaaaa', 8) order by a;

select b from (select 5 as a, 'Hello' as b order by a);
select b from (select 5 as a, 'Hello' as b group by a);
select b from (select 5 as a, 'Hello' as b order by 1);

drop table if exists tp2;
create table tp2(first_col String, second_col Int32) engine = MergeTree() order by tuple();
insert into tp2 select 'bbb', 1;
insert into tp2 select 'aaa', 2;
select count(*) from (select first_col, count(second_col) from tp2 group by 1);
select total from (select first_col, count(second_col) as total from tp2 group by 1);
select first_col from (select first_col, second_col as total from tp2 order by 1 desc);
select first_col from (select first_col, second_col as total from tp2 order by 2 desc);
select max from (select max(first_col) as max, second_col as total from tp2 group by 2) order by 1;
with res as (select first_col from (select first_col, second_col as total from tp2 order by 2 desc) limit 1)
select * from res;

drop table if exists test;
create table test
(
`id`  UInt32,
`time` UInt32,
index `id` (`id`) type set(0) granularity 3,
index `time` (`time`) type minmax granularity 3
) engine = MergeTree()
order by (`time`);

select count(*) as `value`, 0 as `data` from test group by `data`;

drop table test;
