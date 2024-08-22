-- Tags: no-fasttest

set allow_experimental_json_type = 1;
drop table if exists test;
create table test (json JSON(a Dynamic)) engine=MergeTree order by tuple() settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into test select '{"a" : 42}';
insert into test select '{"a" : [1, 2, 3]}';
optimize table test;
select * from test order by toString(json);
drop table test;

create table test (json JSON(a Dynamic)) engine=MergeTree order by tuple() settings min_rows_for_wide_part=10000000, min_bytes_for_wide_part=10000000;
insert into test select '{"a" : 42}';
insert into test select '{"a" : [1, 2, 3]}';
optimize table test;
select * from test order by toString(json);
drop table test;
