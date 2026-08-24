drop table if exists alter_ttl;

SET allow_suspicious_ttl_expressions = 1;

create table alter_ttl(i Int) engine = MergeTree order by i ttl toDate('2020-05-05');
alter table alter_ttl add column s String;
alter table alter_ttl modify column s String ttl toDate('2020-01-01');
show create table alter_ttl;
drop table alter_ttl;

create table alter_ttl(d Date, s String) engine = MergeTree order by d ttl d + interval 1 month;
alter table alter_ttl modify column s String ttl d + interval 1 day;
show create table alter_ttl;
drop table alter_ttl;
