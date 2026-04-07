SET joined_subquery_requires_alias = 0;
SET enable_analyzer = 1;

drop table if exists tab1;
drop table if exists tab2;
drop table if exists tab3;
drop table if exists tab1_copy;

create table tab1 (a1 Int32, b1 Int32) engine = MergeTree order by a1;
create table tab2 (a2 Int32, b2 Int32) engine = MergeTree order by a2;
create table tab3 (a3 Int32, b3 Int32) engine = MergeTree order by a3;
create table tab1_copy (a1 Int32, b1 Int32) engine = MergeTree order by a1;

insert into tab1 values (1, 2);
insert into tab2 values (2, 3);
insert into tab3 values (2, 3);
insert into tab1_copy values (2, 3);


select 'joind columns from right table';
select a1 from tab1 any left join tab2 on b1 = a2;
select a1, b1 from tab1 any left join tab2 on b1 = a2;
select a1, a2 from tab1 any left join tab2 on b1 = a2;
select a1, b2 from tab1 any left join tab2 on b1 = a2;
select a1, a2, b2 from tab1 any left join tab2 on b1 = a2;


select 'join on expression';
select b1 from tab1 any left join tab2 on toInt32(a1 + 1) = a2;
select b1, a2 from tab1 any left join tab2 on toInt32(a1 + 1) = a2;
select b1, b2 from tab1 any left join tab2 on toInt32(a1 + 1) = a2;
select a1 from tab1 any left join tab2 on b1 + 1 = a2 + 1;
select a2 from tab1 any left join tab2 on b1 + 1 = a2 + 1;
select a1, b1, a2, b2 from tab1 any left join tab2 on b1 + 1 = a2 + 1;
select a1, b1, a2, b2, a2 + 1 from tab1 any left join tab2 on b1 + 1 = a2 + 1;
select a1, b1, a2, b2 from tab1 any left join tab2 on a1 + 4 = b2 + 2;


select 'join on and chain';
select a2, b2 from tab2 any left join tab3 on a2 = a3 and b2 = b3;
select a3, b3 from tab2 any left join tab3 on a2 = a3 and b2 = b3;
select a2, b2, a3, b3 from tab2 any left join tab3 on a2 = a3 and b2 = b3;
select a1 from tab1 any left join tab2 on b1 + 1 = a2 + 1 and a1 + 4 = b2 + 2;
select a1, b2 from tab1 any left join tab2 on b1 + 1 = a2 + 1 and a1 + 4 = b2 + 2;
select a1, b1, a2, b2 from tab1 any left join tab2 on b1 + 1 = a2 + 1 and a1 + 4 = b2 + 2;
select a2, b2 + 1 from tab1 any left join tab2 on b1 + 1 = a2 + 1 and a1 + 4 = b2 + 2;


select 'join on aliases';
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on first.b1 = second_.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on second_.a2 = first.b1;

select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab1.b1 = tab2.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab2.a2 = tab1.b1;

select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab1.b1 = tab2.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab2.a2 = tab1.b1;

select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on first.b1 = tab2.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab2.a2 = first.b1;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on first.b1 = tab2.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab2.a2 = first.b1;

select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab1.b1 = second_.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on second_.a2 = tab1.b1;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on tab1.b1 = second_.a2;
select a1, a2, b1, b2 from tab1 first any left join tab2 second_ on second_.a2 = tab1.b1;

select a1, a2, first.b1, second_.b2 from tab1 first any left join tab2 second_ on b1 = a2;
select a1, a2, tab1.b1, tab2.b2 from tab1 first any left join tab2 second_ on b1 = a2;
select a1, a2, tab1.b1, tab2.b2 from tab1 first any left join tab2 second_ on b1 = a2;


select 'join on complex expression';
select a2, b2 from tab2 any left join tab3 on a2 + b2 = a3 + b3;
select a2, b2 from tab2 any left join tab3 on a3 + tab3.b3 = a2 + b2;
select a2, b2 from tab2 second_ any left join tab3 on a3 + b3 = a2 + second_.b2;
select a2, b2 from tab2 second_ any left join tab3 third on third.a3 + tab3.b3 = tab2.a2 + second_.b2;
select a2, b2 from tab2 second_ any left join tab3 third on third.a3 + tab3.b3 = tab2.a2 + second_.b2;

select 'duplicate column names';
select a1, tab1_copy.a1 from tab1 any left join tab1_copy on tab1.b1 + 3 = tab1_copy.b1 + 2 FORMAT JSONEachRow;
select a1, tab1_copy.a1 from tab1 any left join tab1_copy on tab1.b1 + 3 = tab1_copy.b1 + 2 FORMAT JSONEachRow;
select a1, copy.a1 from tab1 any left join tab1_copy copy on tab1.b1 + 3 = tab1_copy.b1 + 2 FORMAT JSONEachRow;
select a1, tab1_copy.a1 from tab1 any left join tab1_copy copy on tab1.b1 + 3 = tab1_copy.b1 + 2 FORMAT JSONEachRow;
select a1, tab1_copy.a1 from tab1 any left join tab1_copy copy on tab1.b1 + 3 = tab1_copy.b1 + 2 FORMAT JSONEachRow;

select 'subquery';
select a1 from tab1 any left join (select * from tab2) on b1 = a2;
select a1 from tab1 any left join (select a2 from tab2) on b1 = a2;
select a1, b1 from tab1 any left join (select * from tab2) on b1 = a2;
select a1, b1, a2, b2 from tab1 any left join (select * from tab2) on b1 = a2;
select a1, a2 from tab1 any left join (select a2 from tab2) on b1 = a2;

select 'subquery expression';
select b1 from tab1 any left join (select * from tab2) on toInt32(a1 + 1) = a2;
select a1, b1, a2, b2 from tab1 any left join (select * from tab2) on b1 + 1 = a2 + 1;
select a1, b1, a2 from tab1 any left join (select * from tab2) on b1 + 1 = a2 + 1;

select 'subquery column alias';
select a1, b1, a2, b2 from tab1 any left join (select *, a2 as z from tab2) on b1 + 1 = z + 1;
select a1, b1, a2, b2 from tab1 any left join (select *, a2 + 1 as z from tab2) on b1 + 1 = z;
select a1, b1, a2, b2 from tab1 any left join (select *, a2 + 1 as z from tab2) on b1 + 2 = z + 1 format TSV;

select 'subquery alias';
select a1, a2, b1, b2 from tab1 first any left join (select * from tab2) second_ on first.b1 = second_.a2;
select a1, a2, b1, b2 from tab1 first any left join (select *, a2 as z from tab2) second_ on first.b1 = second_.z;
select a1, a2, b1, b2 from tab1 first any left join (select *, a2 + 1 as z from tab2) second_ on first.b1 + 1 = second_.z;
select tab1.a1, a2, tab1.b1, second_.b2 from tab1 first any left join (select * from tab2) second_ on first.b1 = second_.a2;
select a1, s.a1 from tab1 any left join (select * from tab1_copy) s on tab1.b1 + 3 = s.b1 + 2 FORMAT JSONEachRow;

drop table tab1;
drop table tab1_copy;
drop table tab2;
drop table tab3;
