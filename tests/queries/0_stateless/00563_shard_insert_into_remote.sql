-- Tags: shard

drop table if exists tab;
create table tab (val UInt8) engine = MergeTree order by val;
insert into function remote('127.0.0.2', currentDatabase(), tab) values (1);
insert into function remote('127.0.0.{2|3}', currentDatabase(), tab) values (2);
insert into function remote('127.0.0.{2|3|4}', currentDatabase(), tab) values (3);
select * from tab order by val;
drop table tab;
