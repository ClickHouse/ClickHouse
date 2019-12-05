SET check_query_single_value_result = 0;
DROP TABLE IF EXISTS check_query_test;

CREATE TABLE check_query_test (SomeKey UInt64, SomeValue String) ENGINE = MergeTree() ORDER BY SomeKey;

-- Number of rows in last granule should be equals to granularity.
-- Rows in this table are short, so granularity will be 8192.
INSERT INTO check_query_test SELECT number, toString(number) FROM system.numbers LIMIT 81920;

CHECK TABLE check_query_test;

OPTIMIZE TABLE check_query_test;

CHECK TABLE check_query_test;

DROP TABLE IF EXISTS check_query_test;
