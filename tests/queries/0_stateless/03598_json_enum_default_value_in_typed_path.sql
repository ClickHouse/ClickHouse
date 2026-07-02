drop table if exists test;
create table test (json JSON(e Enum('a' = 1, 'b' = 2))) engine=MergeTree order by tuple() SETTINGS optimize_row_order_if_no_order_by = 0;
insert into test values ('{"e" : "a"}'), ('{"e" : "b"}'), ('{"e" : null}'), ('{}');
select json from test;
select json.e from test;
drop table test;

