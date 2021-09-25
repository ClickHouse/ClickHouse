SET joined_subquery_requires_alias = 0;
select * FROM (SELECT 1), (SELECT 1), (SELECT 1); -- { serverError 352 }

-- This queries work by luck.
-- Feel free to remove then if it is the only failed test.
select * from (select 2), (select 1) as a, (select 1) as b;
select * from (select 1) as a, (select 2), (select 1) as b;
select * from (select 1) as a, (select 1) as b, (select 2);
