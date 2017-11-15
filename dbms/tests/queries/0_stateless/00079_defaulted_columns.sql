drop table if exists defaulted_test;

create table defaulted_test (col1 default 0) engine=Memory;
desc table defaulted_test;
drop table defaulted_test;

create table defaulted_test (col1 UInt32, col2 default col1 + 1, col3 materialized col1 + 2, col4 alias col1 + 3) engine=Memory;
desc table defaulted_test;
insert into defaulted_test (col1) values (10);
select * from defaulted_test;
select col3, col4 from defaulted_test;
drop table defaulted_test;

create table defaulted_test (col1 Int8, col2 UInt64 default (SELECT dummy+99 from system.one)) engine=Memory;
insert into defaulted_test (col1) values (0);
select col2 from defaulted_test;
drop table defaulted_test;

create table defaulted_test (payload String, date materialized today(), key materialized 0 * rand()) engine=MergeTree(date, key, 8192);
desc table defaulted_test;
insert into defaulted_test (payload) values ('hello clickhouse');
select * from defaulted_test;
alter table defaulted_test add column payload_length materialized length(payload);
desc table defaulted_test;
select *, payload_length from defaulted_test;
insert into defaulted_test (payload) values ('some string');
select *, payload_length from defaulted_test order by payload;
select *, payload_length from defaulted_test order by payload;
alter table defaulted_test modify column payload_length default length(payload);
desc table defaulted_test;
select * from defaulted_test order by payload;
alter table defaulted_test modify column payload_length default length(payload) % 65535;
desc table defaulted_test;
select * from defaulted_test order by payload;
alter table defaulted_test modify column payload_length UInt16 default length(payload);
desc table defaulted_test;
alter table defaulted_test drop column payload_length;
desc table defaulted_test;
select * from defaulted_test order by payload;
drop table defaulted_test;
