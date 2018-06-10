DROP TABLE IF EXISTS test.t;

create table test.t(d Date) engine MergeTree(d, d, 8192);

insert into test.t values ('2018-02-20');

select count() from test.t where toDayOfWeek(d) in (2);

DROP TABLE test.t;
