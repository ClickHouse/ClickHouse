SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_optimize_join_order_limit = 10; -- needed for consistent table labels and row counts in EXPLAIN output

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t3 SELECT number, toString(number) FROM numbers(100);

EXPLAIN PLAN actions = 1, compact=1, pretty = 1
SELECT * FROM t1
INNER JOIN t2 ON t1.id = t2.id;

EXPLAIN PLAN actions = 1, compact=1, pretty = 1
SELECT * FROM t1
INNER JOIN t2 ON t1.id = t2.id
INNER JOIN t3 ON t2.id = t3.id;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;