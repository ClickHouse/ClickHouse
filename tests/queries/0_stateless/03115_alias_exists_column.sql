-- https://github.com/ClickHouse/ClickHouse/issues/44412

SELECT EXISTS(SELECT 1) AS mycheck FORMAT TSVWithNames;