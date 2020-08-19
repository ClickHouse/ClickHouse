DROP TABLE IF EXISTS t_00575;

create table t_00575(d Date) engine MergeTree(d, d, 8192);

insert into t_00575 values ('2018-02-20');

select count() from t_00575 where toDayOfWeek(d) in (2);

DROP TABLE t_00575;
