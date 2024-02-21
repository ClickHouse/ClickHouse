set allow_experimental_variant_type=1;

create table test (v1 Variant(String, UInt64, Array(UInt32)), v2 Variant(String, UInt64, Array(UInt32))) engine=Memory;

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


select 'order by v1 nulls first';
select v1 from test order by v1 nulls first;

select 'order by v1 nulls last';
select v1 from test order by v1 nulls last;

select 'order by v2 nulls first';
select v2 from test order by v2 nulls first;

select 'order by v2 nulls last';
select v2 from test order by v2 nulls last;


select 'order by v1, v2 nulls first';
select * from test order by v1, v2 nulls first;

select 'order by v1, v2 nulls last';
select * from test order by v1, v2 nulls last;

select 'order by v2, v1 nulls first';
select * from test order by v2, v1 nulls first;

select 'order by v2, v1 nulls last';
select * from test order by v2, v1 nulls last;

select 'v1 = v2';
select v1, v2, v1 = v2 from test order by v1, v2;

select 'v1 < v2';
select v1, v2, v1 < v2 from test order by v1, v2;

select 'v1 <= v2';
select v1, v2, v1 <= v2 from test order by v1, v2;

select 'v1 > v2';
select v1, v2, v1 > v2 from test order by v1, v2;

select 'v1 >= v2';
select v1, v2, v2 >= v2 from test order by v1, v2;

drop table test;

