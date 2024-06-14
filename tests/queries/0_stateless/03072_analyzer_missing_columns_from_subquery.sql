-- https://github.com/ClickHouse/ClickHouse/issues/14699
SET allow_experimental_analyzer=1;
select * from (select number from numbers(1)) where not ignore(*);
