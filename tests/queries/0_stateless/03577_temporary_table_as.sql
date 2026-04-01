-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS test;
create table test (a UInt8, b String, c Nullable(Float), date Date) Engine=MergeTree ORDER BY (a,b) PARTITION BY date;
SET default_temporary_table_engine = 'MergeTree';
CREATE TEMPORARY TABLE test3 AS test;
DROP TABLE test;
SHOW CREATE TEMPORARY TABLE test3;
