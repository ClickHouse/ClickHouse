use test;

drop table if exists t1;
drop table if exists t2;

create table t1 (a UInt32, b String) engine = Memory;
create table t2 (c UInt32, d String) engine = Memory;

insert into t1 values (1, 'x'), (2, 'y'), (3, 'z');
insert into t2 values (2, 'w'), (4, 'y');

set enable_optimize_predicate_expression = 0;

select * from t1 join t2 on a = c;
select * from t1 join t2 on c = a;

select t1.a, t2.c from t1 join t2 on a = c;
select t1.a, t2.c from t1 join t2 on c = a;
select t1.b, t2.d from t1 join t2 on a = c;
select t1.b, t2.d from t1 join t2 on c = a;

select a, c from t1 join t2 on a = c;
select a, c from t1 join t2 on c = a;
select b, d from t1 join t2 on a = c;
select b, d from t1 join t2 on c = a;

select b as a, d as c from t1 join t2 on a = c;
select b as a, d as c from t1 join t2 on c = a;
select b as c, d as a from t1 join t2 on a = c;
select b as c, d as a from t1 join t2 on c = a;

select t1.a as a, t2.c as c from t1 join t2 on a = c;
select t1.a as a, t2.c as c from t1 join t2 on c = a;
select t1.a as c, t2.c as a from t1 join t2 on a = c;
select t1.a as c, t2.c as a from t1 join t2 on c = a;
 
select t1.a as c, t2.c as a from t1 join t2 on t1.a = t2.c;
select t1.a as c, t2.c as a from t1 join t2 on t2.c = t1.a;

set enable_optimize_predicate_expression = 1;

select * from t1 join t2 on a = c;
select * from t1 join t2 on c = a;

select t1.a, t2.c from t1 join t2 on a = c;
select t1.a, t2.c from t1 join t2 on c = a;
select t1.b, t2.d from t1 join t2 on a = c;
select t1.b, t2.d from t1 join t2 on c = a;

select a, c from t1 join t2 on a = c;
select a, c from t1 join t2 on c = a;
select b, d from t1 join t2 on a = c;
select b, d from t1 join t2 on c = a;

select b as a, d as c from t1 join t2 on a = c;
select b as a, d as c from t1 join t2 on c = a;
select b as c, d as a from t1 join t2 on a = c;
select b as c, d as a from t1 join t2 on c = a;

select t1.a as a, t2.c as c from t1 join t2 on a = c;
select t1.a as a, t2.c as c from t1 join t2 on c = a;
select t1.a as c, t2.c as a from t1 join t2 on a = c;
select t1.a as c, t2.c as a from t1 join t2 on c = a;
 
select t1.a as c, t2.c as a from t1 join t2 on t1.a = t2.c;
select t1.a as c, t2.c as a from t1 join t2 on t2.c = t1.a;

drop table t1;
drop table t2;
