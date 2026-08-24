drop table if exists test;

create table test (Printer LowCardinality(String), IntervalStart DateTime) engine MergeTree partition by (hiveHash(Printer), toYear(IntervalStart)) order by (Printer, IntervalStart);

insert into test values ('printer1', '2006-02-07 06:28:15');

select Printer from test where Printer='printer1';

drop table test;
