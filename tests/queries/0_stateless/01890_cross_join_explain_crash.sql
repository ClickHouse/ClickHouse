SET enable_analyzer = 1;
SET joined_subquery_requires_alias = 0;

select * FROM (SELECT 1), (SELECT 1), (SELECT 1);
select * from (select 2), (select 1) as a, (select 1) as b;
select * from (select 1) as a, (select 2), (select 1) as b;
select * from (select 1) as a, (select 1) as b, (select 2);
