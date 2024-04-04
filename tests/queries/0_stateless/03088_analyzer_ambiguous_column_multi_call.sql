-- https://github.com/ClickHouse/ClickHouse/issues/61014
SET allow_experimental_analyzer=1;
create database test_03088;

create table test_03088.a (i int) engine = Log();

select
  test_03088.a.i
from
  test_03088.a,
  test_03088.a as x;
