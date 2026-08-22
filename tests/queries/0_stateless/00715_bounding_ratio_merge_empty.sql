drop table if exists rate_test;
drop table if exists rate_test2;

create table rate_test (timestamp UInt32, event UInt32) engine=Memory;
insert into rate_test values (0,1000),(1,1001),(2,1002),(3,1003),(4,1004),(5,1005),(6,1006),(7,1007),(8,1008);

create table rate_test2 (timestamp UInt32, event UInt32) engine=Memory;

SELECT boundingRatioMerge(state) FROM (
  select boundingRatioState(timestamp, event) as state from rate_test
  UNION ALL
  SELECT boundingRatioState(timestamp, event) FROM rate_test2 WHERE 1=0
);

drop table if exists rate_test;
drop table if exists rate_test2;
