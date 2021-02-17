drop table if exists tsgroupsum_test;

create table tsgroupsum_test (uid UInt64, ts Int64, value Float64) engine=Memory;
insert into tsgroupsum_test values (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5);
insert into tsgroupsum_test values (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

select timeSeriesGroupSum(uid, ts, value) from (select * from tsgroupsum_test order by ts asc);
select timeSeriesGroupRateSum(uid, ts, value) from (select * from tsgroupsum_test order by ts asc);

drop table tsgroupsum_test;
