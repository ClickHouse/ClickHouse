drop table if exists aliases_test;

set allow_deprecated_syntax_for_merge_tree=1;
create table aliases_test (date default today(), id default rand(), array default [0, 1, 2]) engine=MergeTree(date, id, 1);

insert into aliases_test (id) values (0);
select array from aliases_test;

alter table aliases_test modify column array alias [0, 1, 2];
select array from aliases_test;

alter table aliases_test modify column array default [0, 1, 2];
select array from aliases_test;

alter table aliases_test add column struct.key Array(UInt8) default [0, 1, 2], add column struct.value Array(UInt8) default array;
select struct.key, struct.value from aliases_test;

alter table aliases_test modify column struct.value alias array;
select struct.key, struct.value from aliases_test;

select struct.key, struct.value from aliases_test array join struct;
select struct.key, struct.value from aliases_test array join struct as struct;
select class.key, class.value from aliases_test array join struct as class;

drop table aliases_test;
