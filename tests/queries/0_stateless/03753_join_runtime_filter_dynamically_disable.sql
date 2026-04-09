CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32, c_nationkey_copy Int32) ENGINE MergeTree ORDER BY c_custkey SETTINGS index_granularity=10;

SYSTEM STOP MERGES customer;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY'),(100,'UNKNOWN');

INSERT INTO customer SELECT number, 5, 5 FROM numbers(1000);
INSERT INTO customer SELECT number, 6, 6 FROM numbers(1000);
INSERT INTO customer SELECT number, 7, 7 FROM numbers(1000);
INSERT INTO customer SELECT number, 100, 100 FROM numbers(10);

SET enable_analyzer=1;
SET enable_join_runtime_filters=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';
SET query_plan_optimize_join_order_algorithm='greedy';
SET query_plan_optimize_join_order_limit=1;
SET query_plan_join_swap_table=0;

-- 1 row in filter
SELECT count()
FROM customer, nation
WHERE
    c_nationkey = n_nationkey
    AND n_name = 'FRANCE'
    AND c_nationkey_copy = 6
SETTINGS join_runtime_filter_exact_values_limit = 100, max_block_size=10, max_threads=1, log_comment='Q1';

-- Check that most of the blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 * ProfileEvents['RuntimeFilterBlocksProcessed'] AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q1'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- 2 rows in exact set
SELECT count()
FROM customer, nation
WHERE
    c_nationkey = n_nationkey
    AND n_name IN ('FRANCE', 'GERMANY')
    AND c_nationkey_copy IN (6, 7)
SETTINGS join_runtime_filter_exact_values_limit = 100, max_block_size=10, max_threads=1, log_comment='Q2';


-- Check that most of the blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 * ProfileEvents['RuntimeFilterBlocksProcessed'] AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q2'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- 3 rows in bloom filter
SELECT count()
FROM customer, nation
WHERE
    c_nationkey = n_nationkey
    AND n_name IN ('FRANCE', 'GERMANY', 'ETHIOPIA')
    AND c_nationkey_copy IN (6, 7, 5)
SETTINGS join_runtime_filter_exact_values_limit=1, max_block_size=10, max_threads=1, log_comment='Q3';


-- Check that most of the blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 * ProfileEvents['RuntimeFilterBlocksProcessed'] AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q3'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- Too many rows in bloom filter
SELECT count()
FROM customer, numbers(2000) AS n
WHERE
    c_nationkey = n.number::Int32
SETTINGS join_runtime_filter_exact_values_limit=1, join_runtime_bloom_filter_bytes=100, max_block_size=10, max_threads=1, log_comment='Q4';


-- Check all blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 AND ProfileEvents['RuntimeFilterBlocksProcessed'] = 0 AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q4'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- ANTI JOIN with 1 row
SELECT count()
FROM (SELECT * FROM customer WHERE c_nationkey_copy != 6) AS c LEFT ANTI JOIN (SELECT * FROM nation WHERE n_name = 'FRANCE') AS n
ON
    c.c_nationkey = n.n_nationkey
SETTINGS join_runtime_filter_exact_values_limit = 100, max_block_size=10, max_threads=1, log_comment='Q5';


-- Check that most of the blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 * ProfileEvents['RuntimeFilterBlocksProcessed'] AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q5'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- ANTI JOIN with exact set
SELECT count()
FROM (SELECT * FROM customer WHERE c_nationkey_copy NOT IN (6,7)) AS c LEFT ANTI JOIN (SELECT * FROM nation WHERE n_name IN ('FRANCE', 'GERMANY')) AS n
ON
    c.c_nationkey = n.n_nationkey
SETTINGS join_runtime_filter_exact_values_limit = 100, max_block_size=10, max_threads=1, log_comment='Q6';


-- Check that most of the blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] > 10 * ProfileEvents['RuntimeFilterBlocksProcessed'] AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q6'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;


-- Change pass ratio to >1 to turn of auto-disabling
SELECT count()
FROM customer, nation
WHERE
    c_nationkey = n_nationkey
    AND n_name = 'FRANCE'
    AND c_nationkey_copy = 6
SETTINGS join_runtime_filter_pass_ratio_threshold_for_disabling=3.14, max_block_size=10, max_threads=1, log_comment='Q7';


-- Check than no blocks were skipped
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['RuntimeFilterBlocksSkipped'] = 0 AND ProfileEvents['RuntimeFilterBlocksProcessed'] > 0 AS Passed,
    if (Passed, 'Ok', query_id || ' : ' || ProfileEvents::String)
FROM system.query_log
WHERE
   type = 'QueryFinish' AND log_comment='Q7'
   AND current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE;
