-- Squashing of parallel_hash and grace_hash output merges blocks only up to max_joined_block_size_rows/bytes.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_l2;
DROP TABLE IF EXISTS t_r2;
DROP TABLE IF EXISTS t_r3;

CREATE TABLE t_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_l2 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r2 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r3 (k UInt64, s String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_l SELECT number FROM numbers(20);
INSERT INTO t_r SELECT number % 20, number FROM numbers(2000);
INSERT INTO t_l2 SELECT number FROM numbers(400);
INSERT INTO t_r2 SELECT number % 400 FROM numbers(2000);
INSERT INTO t_r3 SELECT number, repeat('x', 1000) FROM numbers(180)
WHERE number % 20 < [3, 12, 4, 4, 4, 8, 9, 2, 5][intDiv(number, 20) + 1];

SET max_threads = 4, parallel_hash_join_threshold = 0, query_plan_join_swap_table = 0, joined_block_split_single_row = 1,
    min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288, query_plan_join_shard_by_pk_ranges = 0,
    max_bytes_before_external_join = 0, query_plan_optimize_join_order_randomize = 0, enable_parallel_replicas = 0;

SELECT max(blockSize()) <= 9, count() FROM t_l JOIN t_r ON t_l.k = t_r.k
SETTINGS join_algorithm = 'parallel_hash', max_joined_block_size_rows = 9;

SELECT max(blockSize()) <= 9, count() FROM t_l2 JOIN t_r2 ON t_l2.k = t_r2.k
SETTINGS join_algorithm = 'parallel_hash', max_joined_block_size_rows = 9;

SELECT max(blockSize()) <= 9, count() FROM t_l JOIN t_r ON t_l.k = t_r.k
SETTINGS join_algorithm = 'grace_hash', max_joined_block_size_rows = 9;

SELECT max(blockSize()) BETWEEN 101 AND 200, sum(t_r.v), count() FROM t_l JOIN t_r ON t_l.k = t_r.k
SETTINGS join_algorithm = 'parallel_hash', max_joined_block_size_rows = 65409, max_joined_block_size_bytes = 1600,
    enable_lazy_columns_replication = 0;

SELECT max(blockSize()) BETWEEN 101 AND 200, sum(c), count() FROM (SELECT k, toUInt64(7) AS c FROM t_l) AS l JOIN t_r ON l.k = t_r.k
SETTINGS join_algorithm = 'parallel_hash', max_joined_block_size_rows = 65409, max_joined_block_size_bytes = 1600;

-- One stream of joined chunks of 3, 12, 4, 4, 4, 8, 9, 2, 5 rows (about 1 KB per row) and a 10-row minimum:
-- chunks merge until the minimum is reached, but not past 15 rows or 16000 bytes.
SELECT groupArray(b) FROM (SELECT blockSize() AS b, rowNumberInBlock() AS n FROM numbers(180) AS l JOIN t_r3 ON l.number = t_r3.k)
WHERE n = 0
SETTINGS join_algorithm = 'parallel_hash', max_threads = 1, max_block_size = 20, enable_join_runtime_filters = 0,
    min_joined_block_size_rows = 10, min_joined_block_size_bytes = 0,
    max_joined_block_size_rows = 15, max_joined_block_size_bytes = 0;

SELECT groupArray(b) FROM (
    SELECT blockSize() AS b, rowNumberInBlock() AS n, t_r3.s AS s FROM numbers(180) AS l JOIN t_r3 ON l.number = t_r3.k)
WHERE n = 0 AND length(s) = 1000
SETTINGS join_algorithm = 'parallel_hash', max_threads = 1, max_block_size = 20, enable_join_runtime_filters = 0,
    min_joined_block_size_rows = 10, min_joined_block_size_bytes = 0,
    max_joined_block_size_rows = 65409, max_joined_block_size_bytes = 16000;

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_l2;
DROP TABLE t_r2;
DROP TABLE t_r3;
