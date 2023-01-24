SET optimize_monotonous_functions_in_order_by = 1;

DROP TABLE IF EXISTS test_order_by;

CREATE TABLE test_order_by (timestamp DateTime, key UInt32) ENGINE=MergeTree() ORDER BY (toDate(timestamp), key);
INSERT INTO test_order_by SELECT now() + toIntervalSecond(number), number % 4 FROM numbers(10000);
OPTIMIZE TABLE test_order_by FINAL;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY timestamp LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY timestamp LIMIT 10;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY toDate(timestamp) LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY toDate(timestamp) LIMIT 10;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY toDate(timestamp), timestamp LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY toDate(timestamp), timestamp LIMIT 10;

DROP TABLE IF EXISTS test_order_by;

CREATE TABLE test_order_by (timestamp DateTime, key UInt32) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_order_by SELECT now() + toIntervalSecond(number), number % 4 FROM numbers(10000);
OPTIMIZE TABLE test_order_by FINAL;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY toDate(timestamp), timestamp LIMIT 10;

DROP TABLE IF EXISTS test_order_by;
