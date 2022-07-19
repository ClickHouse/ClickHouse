-- Tags: replica

create table enum_alter_issue (a Enum8('one' = 1, 'two' = 2)) engine = MergeTree() ORDER BY a;
insert into enum_alter_issue values ('one'), ('two');
alter table enum_alter_issue modify column a Enum8('one' = 1, 'two' = 2, 'three' = 3);
insert into enum_alter_issue values ('one'), ('two');
alter table enum_alter_issue detach partition id 'all';
alter table enum_alter_issue attach partition id 'all';
select * from enum_alter_issue order by a;
drop table enum_alter_issue;
