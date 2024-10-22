DROP TABLE IF EXISTS order_by_all SYNC;
CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
    all UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03210', 'r1') ORDER BY tuple();

INSERT INTO order_by_all VALUES ('B', 3, 10), ('C', NULL, 40), ('D', 1, 20), ('A', 2, 30);

SET allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET allow_experimental_analyzer=1; -- fix has been done only for the analyzer
SET enable_order_by_all = 0;

-- { echoOn }
SELECT a, b, all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = 0, allow_experimental_parallel_reading_from_replicas=0;
SELECT a, b, all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = 0, allow_experimental_parallel_reading_from_replicas=1;

DROP TABLE order_by_all SYNC;
