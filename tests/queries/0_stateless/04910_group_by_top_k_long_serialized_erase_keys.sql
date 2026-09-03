-- Erase keys captured from the serialized aggregation method must stay valid
-- after `emplaceKey` returns.  Keys longer than 128 bytes disable batch
-- serialization (on x86), so the serialized key bytes live in the hashing
-- state's scratch buffer rather than in a per-block buffer; a descending key
-- stream then makes the heap evict continuously, erasing hash-table entries
-- through those captured keys.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
-- In-order aggregation bypasses the heap the assertions observe.
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS t_long_serialized;
CREATE TABLE t_long_serialized (s String, n UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Lexically descending 148-byte string keys: every new key is better under
-- `ORDER BY s ASC`, so the heap admits and evicts on every trim.
INSERT INTO t_long_serialized
SELECT concat(repeat('x', 140), leftPad(toString(1000000 - number), 8, '0')), number % 4
FROM numbers(100000)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000;

DROP TABLE IF EXISTS gt_long_serialized;
CREATE TABLE gt_long_serialized ENGINE = Memory EMPTY AS
SELECT s, n, count() FROM t_long_serialized GROUP BY s, n ORDER BY s ASC, n ASC LIMIT 100;

INSERT INTO gt_long_serialized
SELECT s, n, count() FROM t_long_serialized GROUP BY s, n ORDER BY s ASC, n ASC LIMIT 100
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE IF EXISTS opt_long_serialized;
CREATE TABLE opt_long_serialized ENGINE = Memory EMPTY AS
SELECT s, n, count() FROM t_long_serialized GROUP BY s, n ORDER BY s ASC, n ASC LIMIT 100;

INSERT INTO opt_long_serialized
SELECT s, n, count() FROM t_long_serialized GROUP BY s, n ORDER BY s ASC, n ASC LIMIT 100;

-- Standalone execution for the profile-event assertion: a `log_comment` inside
-- a set-operation operand does not reach the statement's query_log entry.
SELECT s, n, count() FROM t_long_serialized GROUP BY s, n ORDER BY s ASC, n ASC LIMIT 100
SETTINGS log_comment = '04910_long_serialized' FORMAT Null;

-- Compare both directions and the cardinalities: a one-sided `EXCEPT` stays
-- empty when the optimized query drops winners and returns a strict subset of
-- the correct top-100, which is exactly the mis-pruning this test pins down.
SELECT 'results match the unoptimized aggregation';
SELECT
    (SELECT count() FROM (SELECT * FROM opt_long_serialized EXCEPT SELECT * FROM gt_long_serialized)),
    (SELECT count() FROM (SELECT * FROM gt_long_serialized EXCEPT SELECT * FROM opt_long_serialized)),
    (SELECT count() FROM opt_long_serialized),
    (SELECT count() FROM gt_long_serialized);

SYSTEM FLUSH LOGS query_log;

SELECT 'the heap pruned through the captured keys';
SELECT max(ProfileEvents['AggregationTopKKeysPruned']) > 10000
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04910_long_serialized';

DROP TABLE opt_long_serialized;
DROP TABLE gt_long_serialized;
DROP TABLE t_long_serialized;
