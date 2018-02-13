drop table if exists test.aliases_test;

create table test.aliases_test (date default today(), id default rand(), array default [0, 1, 2]) engine=MergeTree(date, id, 1);

insert into test.aliases_test (id) values (0);
select array from test.aliases_test;

alter table test.aliases_test modify column array alias [0, 1, 2];
select array from test.aliases_test;

alter table test.aliases_test modify column array default [0, 1, 2];
select array from test.aliases_test;

alter table test.aliases_test add column struct.key default [0, 1, 2], add column struct.value default array;
select struct.key, struct.value from test.aliases_test;

alter table test.aliases_test modify column struct.value alias array;
select struct.key, struct.value from test.aliases_test;

select struct.key, struct.value from test.aliases_test array join struct;
select struct.key, struct.value from test.aliases_test array join struct as struct;
select class.key, class.value from test.aliases_test array join struct as class;

drop table test.aliases_test;
