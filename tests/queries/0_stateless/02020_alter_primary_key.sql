drop table if exists r;

-- create table r (id int, a int, b int, c int, d int) engine ReplicatedMergeTree('/r', '1') order by a;
create table r (t int, a int, b int, c int, d int) engine MergeTree order by a;

insert into r (t, a, b, c, d) values (0, 1, 2, 3, 4);

alter table r modify primary key b;

show create r;

insert into r (t, a, b, c, d) values (1, 1, 2, 3, 4);

alter table r modify primary key c;

insert into r (t, a, b, c, d) values (2, 1, 2, 3, 4);

alter table r modify primary key d;

insert into r (t, a, b, c, d) values (3, 1, 2, 3, 4);

alter table r modify primary key b order by (b, c);

insert into r (t, a, b, c, d) values (4, 1, 2, 3, 4);

show create r;

set force_primary_key = 1;

select * from r where a = 1 order by t;
select * from r where b = 2 order by t;
select * from r where c = 3 order by t;
select * from r where d = 4 order by t;

drop table r;

drop table if exists r1;
drop table if exists r2;

create table r1 (t int, a int, b int, c int, d int) engine ReplicatedMergeTree('/clickhouse/tables/{database}/test_02020', 'r1') order by a;
create table r2 (t int, a int, b int, c int, d int) engine ReplicatedMergeTree('/clickhouse/tables/{database}/test_02020', 'r2') order by a;

insert into r1 (t, a, b, c, d) values (0, 1, 2, 3, 4);

alter table r1 modify primary key b;

show create r1;

insert into r1 (t, a, b, c, d) values (1, 1, 2, 3, 4);

alter table r1 modify primary key c;

insert into r1 (t, a, b, c, d) values (2, 1, 2, 3, 4);

alter table r1 modify primary key d;

insert into r1 (t, a, b, c, d) values (3, 1, 2, 3, 4);

alter table r1 modify primary key b order by (b, c);

insert into r1 (t, a, b, c, d) values (4, 1, 2, 3, 4);

system sync replica r2;

show create r2;

select * from r2 where a = 1 order by t;
select * from r2 where b = 2 order by t;
select * from r2 where c = 3 order by t;
select * from r2 where d = 4 order by t;

drop table if exists r1;
drop table if exists r2;

drop table if exists x;

-- bad cases
create table x (i UInt64, j UInt64) engine MergeTree original primary key i order by j sample by i; -- { serverError 36 }

create table x (i UInt64, j UInt64) engine MergeTree original primary key i order by j;
alter table x modify sample by j; -- { serverError 48 }
drop table x;

create table x (d Date, k UInt64, i32 Int32) engine MergeTree(d, k, 8192);
alter table x modify primary key i32; -- { serverError 36 }
drop table x;
