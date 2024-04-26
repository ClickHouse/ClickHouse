set allow_experimental_dynamic_type = 1;

drop table if exists test;
create table test (d1 Dynamic, d2 Dynamic) engine=Memory;

insert into test values (42, 42);
insert into test values (42, 43);
insert into test values (43, 42);

insert into test values ('abc', 'abc');
insert into test values ('abc', 'abd');
insert into test values ('abd', 'abc');

insert into test values ([1,2,3], [1,2,3]);
insert into test values ([1,2,3], [1,2,4]);
insert into test values ([1,2,4], [1,2,3]);

insert into test values (NULL, NULL);

insert into test values (42, 'abc');
insert into test values ('abc', 42);

insert into test values (42, [1,2,3]);
insert into test values ([1,2,3], 42);

insert into test values (42, NULL);
insert into test values (NULL, 42);

insert into test values ('abc', [1,2,3]);
insert into test values ([1,2,3], 'abc');

insert into test values ('abc', NULL);
insert into test values (NULL, 'abc');

insert into test values ([1,2,3], NULL);
insert into test values (NULL, [1,2,3]);


select 'order by d1 nulls first';
select d1, dynamicType(d1) from test order by d1 nulls first;

select 'order by d1 nulls last';
select d1, dynamicType(d1) from test order by d1 nulls last;

select 'order by d2 nulls first';
select d2, dynamicType(d2) from test order by d2 nulls first;

select 'order by d2 nulls last';
select d2, dynamicType(d2) from test order by d2 nulls last;


select 'order by d1, d2 nulls first';
select d1, d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2 nulls first;

select 'order by d1, d2 nulls last';
select d1, d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2 nulls last;

select 'order by d2, d1 nulls first';
select d1, d2, dynamicType(d1), dynamicType(d2) from test order by d2, d1 nulls first;

select 'order by d2, d1 nulls last';
select d1, d2, dynamicType(d1), dynamicType(d2) from test order by d2, d1 nulls last;

select 'd1 = d2';
select d1, d2, d1 = d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2;

select 'd1 < d2';
select d1, d2, d1 < d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2;

select 'd1 <= d2';
select d1, d2, d1 <= d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2;

select 'd1 > d2';
select d1, d2, d1 > d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2;

select 'd1 >= d2';
select d1, d2, d2 >= d2, dynamicType(d1), dynamicType(d2) from test order by d1, d2;

drop table test;

