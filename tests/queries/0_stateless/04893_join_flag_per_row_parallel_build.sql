-- Multi-disjunct FULL JOIN with a parallel build: unmatched-right counts must be exact
-- (a lost or duplicated per-row flag silently drops or double-counts rows).
-- Right: 200000 rows. [0, 100) match via the first disjunct, [100000, 100100) via the second.
-- matched pairs = 200, unmatched right = 199800, total = 200000.

SET max_threads = 8;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 1;
SET max_block_size = 8192;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t1_04893;
DROP TABLE IF EXISTS t2_04893;

CREATE TABLE t1_04893 (number UInt64) ENGINE = MergeTree ORDER BY number;
CREATE TABLE t2_04893 (number UInt64) ENGINE = MergeTree ORDER BY number;

INSERT INTO t1_04893 SELECT number FROM numbers(100);
INSERT INTO t2_04893 SELECT number FROM numbers(200000);

SELECT count(), sum(t2_04893.number), sum(t1_04893.number)
FROM t1_04893
FULL JOIN t2_04893
    ON t1_04893.number = t2_04893.number OR t1_04893.number + 100000 = t2_04893.number
SETTINGS log_comment = '04893_full_join';

-- The row counts would also match a serial build, so check that the join was built in parallel.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04893_full_join';

DROP TABLE t1_04893;
DROP TABLE t2_04893;
