drop table if exists test;
create table test (map Map(String, DateTime)) engine=Memory;
set date_time_input_format='best_effort';
insert into test values (map('Hello', '01/01/2020'));
select * from test;
drop table test;

