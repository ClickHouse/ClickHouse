drop table if exists test.defaulted;

create table test.defaulted (col1 default 0) engine=Memory;
desc table test.defaulted;
drop table test.defaulted;

create table test.defaulted (col1 UInt32, col2 default col1 + 1, col3 materialized col1 + 2, col4 alias col1 + 3) engine=Memory;
desc table test.defaulted;
insert into test.defaulted (col1) values (10);
select * from test.defaulted;
select col3, col4 from test.defaulted;
drop table test.defaulted;

create table test.defaulted (col1 Int8, col2 UInt64 default (SELECT dummy+99 from system.one)) engine=Memory;
insert into test.defaulted (col1) values (0);
select col2 from test.defaulted;
drop table test.defaulted;

create table test.defaulted (payload String, date materialized today(), key materialized 0 * rand()) engine=MergeTree(date, key, 8192);
desc table test.defaulted;
insert into test.defaulted (payload) values ('hello clickhouse');
select * from test.defaulted;
alter table test.defaulted add column payload_length materialized length(payload);
desc table test.defaulted;
select *, payload_length from test.defaulted;
insert into test.defaulted (payload) values ('some string');
select *, payload_length from test.defaulted order by payload;
select *, payload_length from test.defaulted order by payload;
alter table test.defaulted modify column payload_length default length(payload);
desc table test.defaulted;
select * from test.defaulted order by payload;
alter table test.defaulted modify column payload_length default length(payload) % 65535;
desc table test.defaulted;
select * from test.defaulted order by payload;
alter table test.defaulted modify column payload_length UInt16 default length(payload);
desc table test.defaulted;
alter table test.defaulted drop column payload_length;
desc table test.defaulted;
select * from test.defaulted order by payload;
drop table test.defaulted;
