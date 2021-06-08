SET joined_subquery_requires_alias = 0;
select * FROM (SELECT 1), (SELECT 1), (SELECT 1); -- { serverError 352 }
