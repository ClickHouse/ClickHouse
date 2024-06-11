-- https://github.com/ClickHouse/ClickHouse/issues/65035
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 1000);