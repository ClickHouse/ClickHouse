create table enum_alter_issue (a Enum8('one' = 1, 'two' = 2))
engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02012/enum_alter_issue', 'r1')
ORDER BY a;

insert into enum_alter_issue values ('one'), ('two');
alter table enum_alter_issue modify column a Enum8('one' = 1, 'two' = 2, 'three' = 3);
insert into enum_alter_issue values ('one'), ('two');

alter table enum_alter_issue detach partition id 'all';
alter table enum_alter_issue attach partition id 'all';
select * from enum_alter_issue;

drop table enum_alter_issue;
