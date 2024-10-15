set allow_experimental_dynamic_type = 1;
drop table if exists test;
create table test (d Dynamic(max_types=2)) engine=Memory;
insert into test values (42), ('Hello'), ([1,2,3]), ('2020-01-01');
insert into test values ('Hello'), ([1,2,3]), ('2020-01-01'), (42);
insert into test values ([1,2,3]), ('2020-01-01'), (42), ('Hello');
insert into test values ('2020-01-01'), (42), ('Hello'), ([1,2,3]);
insert into test values (42);
insert into test values ('Hello');
insert into test values ([1,2,3]);
insert into test values ('2020-01-01');

select uniqExact(d) from test;
select count(), d from test group by d order by d;
drop table test;
