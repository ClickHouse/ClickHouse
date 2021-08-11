-- { echo }
set enable_positional_arguments = 1;

drop table if exists test;
create table test (col1 Int32, col2 Int32, col3 Int32) engine = Memory();

insert into test select number, number, 5 from numbers(2);
insert into test select number, number, 4 from numbers(2);
insert into test select number, number, 3 from numbers(2);
insert into test select number, number, 2 from numbers(2);
insert into test select number, number, 1 from numbers(2);

select * from test where col1 = 1 order by 3 desc;
select * from test where col2 = 1 order by 3 asc;

insert into test select number, number+1, 1 from numbers(2);
insert into test select number, number+1, 2 from numbers(2);
insert into test select number, number+1, 3 from numbers(2);
insert into test select number, number+1, 4 from numbers(2);
insert into test select number, number+1, 5 from numbers(2);

select * from test order by col1, col2, col3 asc limit 2 by col2;
select * from test order by 1, 2, 3 asc limit 2 by 2;

select col1, col2 from test group by col1, col2 order by col1, col2;
select col1, col2 from test group by 1, 2 order by 1, 2;

select col2, col3 from test group by col3, col2 order by col3, col2;
select col2, col3 from test group by 3, 2 order by 3, 2;

select col2 from test group by 2 order by 2;
select col2 + 100 from test group by 2 order by 2;
