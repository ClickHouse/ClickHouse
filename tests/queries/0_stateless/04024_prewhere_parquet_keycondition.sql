-- https://github.com/ClickHouse/ClickHouse/issues/96912
INSERT INTO FUNCTION file('04024_data.parquet') SELECT 1;
SELECT 1 FROM file('04024_data.parquet', Parquet, 'c0 Int') PREWHERE c0 = 1;
