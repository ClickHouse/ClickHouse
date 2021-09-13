-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Unsupported type of ALTER query

drop table if exists enum_alter_issue;
create table enum_alter_issue (a Enum16('one' = 1, 'two' = 2), b Int)
engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02012/enum_alter_issue', 'r2')
ORDER BY b;

insert into enum_alter_issue values ('one', 1), ('two', 1);
alter table enum_alter_issue detach partition id 'all';
alter table enum_alter_issue modify column a Enum8('one' = 1, 'two' = 2, 'three' = 3);
insert into enum_alter_issue values ('one', 1), ('two', 1);

alter table enum_alter_issue attach partition id 'all'; -- {serverError TYPE_MISMATCH}
drop table enum_alter_issue;
