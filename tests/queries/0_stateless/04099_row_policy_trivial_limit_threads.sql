-- Tags: no-parallel-replicas, no-object-storage
-- -no-parallel-replicas - PR changes the EXPLAIN output
-- -no-object-storage - parts on remote FS use MergeTreeSelect(pool: ReadPool) over MergeTreeSelect(pool: ReadPoolInOrder)

-- The trivial LIMIT optimization reduces max_streams to 1 for simple
-- SELECT ... LIMIT N queries without WHERE.  When row policies or
-- additional table filters are active the optimization must not fire
-- because the filter still needs multi-threaded index analysis.

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;

DROP TABLE IF EXISTS t_row_policy_limit;
CREATE TABLE t_row_policy_limit (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_row_policy_limit;

-- Create enough parts so multiple streams are meaningful.
INSERT INTO t_row_policy_limit SELECT number FROM numbers(1000) SETTINGS insert_deduplicate=1;
INSERT INTO t_row_policy_limit SELECT number FROM numbers(1000) SETTINGS insert_deduplicate=1;
INSERT INTO t_row_policy_limit SELECT number FROM numbers(1000) SETTINGS insert_deduplicate=1;
INSERT INTO t_row_policy_limit SELECT number FROM numbers(1000) SETTINGS insert_deduplicate=1;

SET enable_analyzer = 1;

-- Case 0: no row policies
SELECT 'no_row_policy';
SELECT *
FROM (EXPLAIN PIPELINE SELECT id FROM t_row_policy_limit LIMIT 1
      SETTINGS max_threads = 4)
WHERE explain LIKE '%Limit%' OR explain LIKE '%Concat%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%MergeTreeSelect%';

-- Case 1: row policy prevents trivial-limit, keeps multiple streams.
DROP ROW POLICY IF EXISTS 04099_p1 ON t_row_policy_limit;
CREATE ROW POLICY 04099_p1 ON t_row_policy_limit USING id < 500 AS permissive TO ALL;

SELECT 'row_policy';
SELECT *
FROM (EXPLAIN PIPELINE SELECT id FROM t_row_policy_limit LIMIT 1
      SETTINGS max_threads = 4)
WHERE explain LIKE '%Limit%' OR explain LIKE '%Concat%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%MergeTreeSelect%';
DROP ROW POLICY 04099_p1 ON t_row_policy_limit;

-- Case 2: always-true row policy adds no effective predicate, so the
-- trivial-limit optimization must still fire (max_streams = 1).
DROP ROW POLICY IF EXISTS 04099_p_true ON t_row_policy_limit;
CREATE ROW POLICY 04099_p_true ON t_row_policy_limit USING 1 AS permissive TO ALL;

SELECT 'row_policy_always_true';
SELECT *
FROM (EXPLAIN PIPELINE SELECT id FROM t_row_policy_limit LIMIT 1
      SETTINGS max_threads = 4)
WHERE explain LIKE '%Limit%' OR explain LIKE '%Concat%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%MergeTreeSelect%';

DROP ROW POLICY 04099_p_true ON t_row_policy_limit;

-- Case 3: additional_table_filters prevents trivial-limit.
SELECT 'additional_filter';
SELECT *
FROM (EXPLAIN PIPELINE SELECT id FROM t_row_policy_limit LIMIT 1
      SETTINGS max_threads = 4,
               additional_table_filters = {'t_row_policy_limit': 'id < 500'})
WHERE explain LIKE '%Limit%' OR explain LIKE '%Concat%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%MergeTreeSelect%';

DROP TABLE t_row_policy_limit;
