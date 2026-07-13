drop table if exists test;
create table test (x UInt32, arr1 Array(UInt32), arr2 Array(UInt32)) engine=MergeTree order by x;
insert into test values (1, [0, 1], [0, 1]), (2, [0], [0, 1]);
select * from test where x == 1 and arrayExists((x1, x2) -> (x1 == x2), arr1, arr2) settings allow_reorder_prewhere_conditions = 0;
drop table test;

