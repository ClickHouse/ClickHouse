
drop table if exists tst2;
create table tst2
(
	timestamp DateTime,
	val Nullable(Int8)
) engine MergeTree partition by toYYYYMM(timestamp) ORDER by (timestamp);

insert into tst2 values ('2018-02-01 00:00:00', 1), ('2018-02-02 00:00:00', 2);

select * from tst2;
select count() from tst2 where val is not null;
select count() from tst2 where timestamp is not null;
select count(*) from tst2 where timestamp > '2017-01-01 00:00:00'

drop table tst2;
