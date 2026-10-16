-- https://github.com/ClickHouse/ClickHouse/issues/14699
select * from (select number from numbers(1)) where not ignore(*);
