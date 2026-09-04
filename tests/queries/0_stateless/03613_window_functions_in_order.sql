-- Tags: no-parallel-replicas
-- Optimization doen't work with parallel replicas

CREATE TABLE t(a UInt32, b UInt32) ENGINE=MergeTree() ORDER BY a SETTINGS index_granularity = 8192;

INSERT INTO t SELECT number, number % 12345 FROM numbers_mt(1e7);

SET max_threads=4, optimize_read_in_order=1;

SELECT countIf(explain like '%ScatterByPartitionTransform%')
FROM (
    EXPLAIN pipeline
    SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions=0
);

SELECT countIf(explain like '%ScatterByPartitionTransform%')
FROM (
    EXPLAIN pipeline
    SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions=1
);
