-- Verify that creating a skip index with an empty expression is rejected.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=e773da12ff7fd4f1e855b298b829898e001d4d32&name_0=MasterCI&name_1=BuzzHouse%20%28arm_asan%29

CREATE TABLE t_empty_index (c0 Int32, INDEX i0 () TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY c0; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_empty_index (c0 Int32, INDEX i0 () TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY c0; -- { serverError INCORRECT_QUERY }
