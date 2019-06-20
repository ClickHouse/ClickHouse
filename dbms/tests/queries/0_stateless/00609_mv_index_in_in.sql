DROP TABLE IF EXISTS test_00609;
DROP TABLE IF EXISTS test_mv_00609;

create table test_00609 (a Int8) engine=Memory;

insert into test_00609 values (1);
create materialized view test_mv_00609 Engine=MergeTree(date, (a), 8192) populate as select a, toDate('2000-01-01') date from test_00609;

select * from test_mv_00609; -- OK
select * from test_mv_00609 where a in (select a from test_mv_00609); -- EMPTY (bug)
select * from ".inner.test_mv_00609" where a in (select a from test_mv_00609); -- OK

DROP TABLE test_00609;
DROP TABLE test_mv_00609;
