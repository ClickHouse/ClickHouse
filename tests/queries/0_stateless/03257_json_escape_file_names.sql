SET enable_json_type = 1;
drop table if exists test;
create table test (json JSON) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
insert into test format JSONAsObject {"a/b/c" : 42, "a-b-c" : 43, "a-b/c-d/e" : 44};

select * from test;
select json.`a/b/c`, json.`a-b-c`, json.`a-b/c-d/e` from test;
select json.`a/b/c`.:Int64, json.`a-b-c`.:Int64, json.`a-b/c-d/e`.:Int64 from test;
drop table test;
