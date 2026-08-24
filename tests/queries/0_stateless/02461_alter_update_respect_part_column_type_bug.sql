drop table if exists src;
create table src( A Int64, B String, C String) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values(1, 'one', 'test');

alter table src detach partition tuple();
alter table src modify column B Nullable(String);
alter table src attach partition tuple();

alter table src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop table if exists src;
create table src( A String, B String, C String) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column A LowCardinality(String);
alter table src attach partition tuple();

alter table src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop table if exists src;
create table src( A String, B String, C String) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column A LowCardinality(String);
alter table src attach partition tuple();

alter table src modify column C LowCardinality(String);
select * from src;

drop table if exists src;
create table src( A String, B String, C String) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column B Nullable(String);
alter table src attach partition tuple();

alter table src rename column B to D;
select * from src;

select '-----';

drop table if exists src;
create table src( A Int64, B String, C String) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src1', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values(1, 'one', 'test');

alter table src detach partition tuple();
alter table src modify column B Nullable(String);
alter table src attach partition tuple();

alter table src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop table if exists src;
create table src( A String, B String, C String) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src2', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column A LowCardinality(String);
alter table src attach partition tuple();

alter table src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop table if exists src;
create table src( A String, B String, C String) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src3', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column A LowCardinality(String);
alter table src attach partition tuple();

alter table src modify column C LowCardinality(String);
select * from src;

drop table if exists src;
create table src( A String, B String, C String) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src4', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter table src detach partition tuple();
alter table src modify column B Nullable(String);
alter table src attach partition tuple();

alter table src rename column B to D;
select * from src;

