drop table if exists t;

create table t (x Int32, codectest Int32) engine = MergeTree order by x;

alter table t add projection x (select * order by codectest);

insert into t values (1, 2);

select * from merge('', 't');

drop table t;
