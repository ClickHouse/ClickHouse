-- Regression for PartialSortingTransform optimization
-- that requires at least 1500 rows.
select * from (select * from (select 0 a, toNullable(number) b, toString(number) c from numbers(1e6)) order by a desc, b desc, c limit 1500) limit 10;
