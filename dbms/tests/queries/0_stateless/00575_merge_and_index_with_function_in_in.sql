DROP TABLE IF EXISTS t;

create table t(d Date) engine MergeTree(d, d, 8192);

insert into t values ('2018-02-20');

select count() from t where toDayOfWeek(d) in (2);

DROP TABLE t;
