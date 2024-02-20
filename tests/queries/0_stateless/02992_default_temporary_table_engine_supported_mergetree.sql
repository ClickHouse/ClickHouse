CREATE TABLE 02992_test (a UInt8, b String, c Nullable(Float), date Date) Engine=MergeTree ORDER BY (a,b) PARTITION BY date;
SET default_temporary_table_engine = 'MergeTree';
CREATE TEMPORARY TABLE 02992_test2 AS 02992_test;
SHOW CREATE TEMPORARY TABLE 02992_test2;
