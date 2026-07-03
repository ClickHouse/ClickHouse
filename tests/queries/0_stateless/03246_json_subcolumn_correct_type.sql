set enable_json_type=1;
set enable_analyzer=1;
set allow_dynamic_type_in_join_keys=1;
drop table if exists test;
create table test (json JSON(max_dynamic_types=1)) engine=Memory;
insert into test values ('{"c0" : 1}'), ('{"c0" : 2}');
select toTypeName(json.c0) from test;
SELECT 1 FROM (SELECT 1 AS c0) tx FULL OUTER JOIN test ON test.json.Float32 = tx.c0;
drop table test;
