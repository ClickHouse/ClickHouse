drop table if exists test_prewhere_column_missing;

create table test_prewhere_column_missing (d default today(), x UInt64) engine=MergeTree(d, x, 1);

insert into test_prewhere_column_missing (x) values (0);
select * from test_prewhere_column_missing;

alter table test_prewhere_column_missing add column arr Array(UInt64);
select * from test_prewhere_column_missing;

select *, arraySum(arr) as s from test_prewhere_column_missing;
select *, arraySum(arr) as s from test_prewhere_column_missing where s = 0;
select *, arraySum(arr) as s from test_prewhere_column_missing prewhere s = 0;

drop table test_prewhere_column_missing;
