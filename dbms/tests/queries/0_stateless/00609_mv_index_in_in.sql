DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_mv;

create table test (a Int8) engine=Memory;

insert into test values (1);
create materialized view test_mv Engine=MergeTree(date, (a), 8192) populate as select a, toDate('2000-01-01') date from test;

select * from test_mv; -- OK
select * from test_mv where a in (select a from test_mv); -- EMPTY (bug)
select * from ".inner.test_mv" where a in (select a from test_mv); -- OK

DROP TABLE test;
DROP TABLE test_mv;
