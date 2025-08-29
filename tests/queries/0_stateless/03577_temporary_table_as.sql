-- Disable this setting to properly test String type
SET data_type_string_use_size_stream = 0;
DROP TABLE IF EXISTS test;
create table test (a UInt8, b String, c Nullable(Float), date Date) Engine=MergeTree ORDER BY (a,b) PARTITION BY date;
SET default_temporary_table_engine = 'MergeTree';
CREATE TEMPORARY TABLE test3 AS test;
DROP TABLE test;
SHOW CREATE TEMPORARY TABLE test3;
