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

-- aggregation in order --

set optimize_aggregation_in_order = 1;

create table t4(a UInt32) engine=MergeTree order by a partition by a % 4;

system stop merges t4;

insert into t4 select number from numbers_mt(1e6);
insert into t4 select number from numbers_mt(1e6);

explain pipeline select a from t4 group by a settings read_in_order_two_level_merge_threshold = 1e12;

select count() from (select throwIf(count() != 2) from t4 group by a);

drop table t4;

create table t5(a UInt32) engine=MergeTree order by a partition by a % 8;

system stop merges t5;

insert into t5 select number from numbers_mt(1e6);
insert into t5 select number from numbers_mt(1e6);

explain pipeline select a from t5 group by a settings read_in_order_two_level_merge_threshold = 1e12;

select count() from (select throwIf(count() != 2) from t5 group by a);

drop table t5;

create table t6(a UInt32) engine=MergeTree order by a partition by a % 16;

system stop merges t6;

insert into t6 select number from numbers_mt(1e6);
insert into t6 select number from numbers_mt(1e6);

explain pipeline select a from t6 group by a settings read_in_order_two_level_merge_threshold = 1e12;

select count() from (select throwIf(count() != 2) from t6 group by a);

drop table t6;
