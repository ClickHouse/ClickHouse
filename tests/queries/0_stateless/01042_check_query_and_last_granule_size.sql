SET optimize_trivial_insert_select = 1;
SET check_query_single_value_result = 0;

DROP TABLE IF EXISTS check_query_test;

CREATE TABLE check_query_test (SomeKey UInt64, SomeValue String) ENGINE = MergeTree() ORDER BY SomeKey SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Number of rows in last granule should be equals to granularity.
-- Rows in this table are short, so granularity will be 8192.
INSERT INTO check_query_test SELECT number, toString(number) FROM system.numbers LIMIT 81920;

CHECK TABLE check_query_test SETTINGS max_threads = 1;

OPTIMIZE TABLE check_query_test;

CHECK TABLE check_query_test SETTINGS max_threads = 1;

DROP TABLE IF EXISTS check_query_test;

DROP TABLE IF EXISTS check_query_test_non_adaptive;

CREATE TABLE check_query_test_non_adaptive (SomeKey UInt64, SomeValue String) ENGINE = MergeTree() ORDER BY SomeKey SETTINGS index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO check_query_test_non_adaptive SELECT number, toString(number) FROM system.numbers LIMIT 81920;

CHECK TABLE check_query_test_non_adaptive SETTINGS max_threads = 1;

OPTIMIZE TABLE check_query_test_non_adaptive;

CHECK TABLE check_query_test_non_adaptive SETTINGS max_threads = 1;

INSERT INTO check_query_test_non_adaptive SELECT number, toString(number) FROM system.numbers LIMIT 77;

CHECK TABLE check_query_test_non_adaptive SETTINGS max_threads = 1;

OPTIMIZE TABLE check_query_test_non_adaptive;

CHECK TABLE check_query_test_non_adaptive SETTINGS max_threads = 1;

DROP TABLE IF EXISTS check_query_test_non_adaptive;
