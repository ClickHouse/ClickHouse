drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a UInt32, b String) engine = Memory;
create table t2 (c UInt32, d String) engine = Memory;
create table t3 (a UInt32) engine = Memory;

insert into t1 values (1, 'x'), (2, 'y'), (3, 'z');
insert into t2 values (2, 'w'), (4, 'y');
insert into t3 values (3);

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

select t1.a, t3.a from t1 join t3 on t1.a = t3.a;
select t1.a as t1_a, t3.a as t3_a from t1 join t3 on t1_a = t3_a;
select table1.a as t1_a, table3.a as t3_a from t1 as table1 join t3 as table3 on t1_a = t3_a;

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

select t1.a, t3.a from t1 join t3 on t1.a = t3.a;
select t1.a as t1_a, t3.a as t3_a from t1 join t3 on t1_a = t3_a;
select table1.a as t1_a, table3.a as t3_a from t1 as table1 join t3 as table3 on t1_a = t3_a;

drop table t1;
drop table t2;
drop table t3;
