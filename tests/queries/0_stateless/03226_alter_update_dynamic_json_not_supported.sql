set allow_experimental_dynamic_type = 1;
set allow_experimental_json_type = 1;

drop table if exists test;
create table test (d Dynamic, json JSON) engine=MergeTree order by tuple();
alter table test update d = 42 where 1; -- {serverError CANNOT_UPDATE_COLUMN}
alter table test update json = '{}' where 1; -- {serverError CANNOT_UPDATE_COLUMN}
drop table test;

