set allow_experimental_json_type = 1;

drop table if exists test;
create table test (s String) engine=MergeTree order by tuple();
alter table test modify column s JSON; -- { serverError BAD_ARGUMENTS }
drop table test;

create table test (s Array(String)) engine=MergeTree order by tuple();
alter table test modify column s Array(JSON); -- { serverError BAD_ARGUMENTS }
drop table test;

create table test (s Tuple(String, String)) engine=MergeTree order by tuple();
alter table test modify column s Tuple(JSON, String); -- { serverError BAD_ARGUMENTS }
drop table test;

