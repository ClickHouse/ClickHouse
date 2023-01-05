set max_threads = 16;

create table t1(a UInt32) engine=MergeTree order by tuple() partition by a % 4;

system stop merges t1;

insert into t1 select number from numbers_mt(1e6);
insert into t1 select number from numbers_mt(1e6);

explain pipeline select a from t1 group by a;

select count() from (select throwIf(count() != 2) from t1 group by a);

drop table t1;

create table t2(a UInt32) engine=MergeTree order by tuple() partition by a % 8;

system stop merges t2;

insert into t2 select number from numbers_mt(1e6);
insert into t2 select number from numbers_mt(1e6);

explain pipeline select a from t2 group by a;

select count() from (select throwIf(count() != 2) from t2 group by a);

drop table t2;

create table t3(a UInt32) engine=MergeTree order by tuple() partition by a % 16;

system stop merges t3;

insert into t3 select number from numbers_mt(1e6);
insert into t3 select number from numbers_mt(1e6);

explain pipeline select a from t3 group by a;

select count() from (select throwIf(count() != 2) from t3 group by a);

select throwIf(count() != 4) from remote('127.0.0.{1,2}', currentDatabase(), t3) group by a format Null;

drop table t3;
