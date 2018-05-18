drop table if exists funnel_test;

create table funnel_test (timestamp UInt32, event UInt32) engine=Memory;
insert into funnel_test values (0,1000),(1,1001),(2,1002),(3,1003),(4,1004),(5,1005),(6,1006),(7,1007),(8,1008);

select 1 = windowFunnel(10000)(timestamp, event = 1000) from funnel_test;
select 2 = windowFunnel(10000)(timestamp, event = 1000, event = 1001) from funnel_test;
select 3 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002) from funnel_test;
select 4 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002, event = 1008) from funnel_test;



select 1 = windowFunnel(1)(timestamp, event = 1000) from funnel_test;
select 3 = windowFunnel(2)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 4 = windowFunnel(3)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;


drop table if exists funnel_test2;
create table funnel_test2 (uid UInt32 default 1,timestamp DateTime, event UInt32) engine=Memory;
insert into funnel_test2(timestamp, event) values  (now() + 1,1001),(now() + 2,1002),(now() + 3,1003),(now() + 4,1004),(now() + 5,1005),(now() + 6,1006),(now() + 7,1007),(now() + 8,1008);


select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;
select 2 = windowFunnel(10000)(timestamp, event = 1001, event = 1008) from funnel_test2;
select 1 = windowFunnel(10000)(timestamp, event = 1008, event = 1001) from funnel_test2;
select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;

drop table funnel_test;
drop table funnel_test2;