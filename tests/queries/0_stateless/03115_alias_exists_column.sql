-- https://github.com/ClickHouse/ClickHouse/issues/44412

SET allow_experimental_analyzer=1;

SELECT EXISTS(SELECT 1) AS mycheck FORMAT TSVWithNames;
