-- A hash join builds its table with the parallel layout when the join kind is INNER, LEFT, RIGHT or
-- FULL and the right side is not estimated below `parallel_hash_join_threshold`. The number of threads
-- does not matter. Each case below builds the serial layout, next to a control that changes one
-- condition and builds the parallel layout. The one-thread case is the other way round.
-- `used_join_algorithms` reports `HASH` for both layouts, so the test counts the tables of each layout
-- with `HashJoinBuiltWithSerialLayout` and `HashJoinBuiltWithParallelLayout`. 05056 tests the hash
-- table statistics of an earlier run.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 4;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS sl;
DROP TABLE IF EXISTS sr;
DROP TABLE IF EXISTS tj;

-- A `Memory` table gives the join an exact estimate: its row count.
CREATE TABLE l (a UInt64) ENGINE = Memory;
CREATE TABLE r (a UInt64) ENGINE = Memory;
INSERT INTO l SELECT number FROM numbers(1000);
INSERT INTO r SELECT number FROM numbers(200);

-- The estimate of 200 rows is below a threshold of 201 and not below a threshold of 200.
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 201, log_comment = '05327_estimate_below' FORMAT Null;
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 200, log_comment = '05327_estimate_equal' FORMAT Null;

-- The join order optimization takes a separate path for SEMI, ANTI and ANY joins: it only swaps
-- their sides. The estimate and the threshold work the same on that path.
SELECT count() FROM l LEFT SEMI JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 201, log_comment = '05327_semi_estimate_below' FORMAT Null;
SELECT count() FROM l LEFT SEMI JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 200, log_comment = '05327_semi_estimate_equal' FORMAT Null;
SELECT count() FROM l ANY LEFT JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 201, log_comment = '05327_any_estimate_below' FORMAT Null;
SELECT count() FROM l ANY LEFT JOIN r ON l.a = r.a
SETTINGS parallel_hash_join_threshold = 200, log_comment = '05327_any_estimate_equal' FORMAT Null;

-- Without the join order optimization the join has no estimate, so on one thread it builds the parallel
-- layout with one slot. The control has the estimate.
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS max_threads = 1, parallel_hash_join_threshold = 201, query_plan_optimize_join_order_limit = 0, log_comment = '05327_one_thread' FORMAT Null;
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS max_threads = 1, parallel_hash_join_threshold = 201, log_comment = '05327_one_thread_control' FORMAT Null;

-- `grace_hash` builds one serial table for each of its 4 buckets, whatever the threshold says.
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1000000000, log_comment = '05327_grace_buckets' FORMAT Null;
SELECT count() FROM l JOIN r ON l.a = r.a
SETTINGS log_comment = '05327_grace_control' FORMAT Null;

-- `query_plan_join_shard_by_pk_ranges` splits both sides by ranges of the primary key and joins
-- each range with its own serial table, one per thread. It applies only to a bare `HashJoin`, so it
-- needs the runtime filters and the spilling threshold that are disabled above.
CREATE TABLE sl (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
CREATE TABLE sr (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO sl SELECT number FROM numbers(100000);
INSERT INTO sr SELECT number FROM numbers(100000);
SELECT count() FROM sl JOIN sr ON sl.a = sr.a
SETTINGS query_plan_join_shard_by_pk_ranges = 1, log_comment = '05327_shard_by_pk' FORMAT Null;
SELECT count() FROM sl JOIN sr ON sl.a = sr.a
SETTINGS log_comment = '05327_shard_control' FORMAT Null;

-- A `Join` engine table keeps a serial table filled by its inserts, and a join with it builds no table.
CREATE TABLE tj (a UInt64) ENGINE = Join(ANY, LEFT, a);
INSERT INTO tj SELECT number FROM numbers(200);
SELECT count() FROM l ANY LEFT JOIN tj ON l.a = tj.a
SETTINGS log_comment = '05327_join_engine' FORMAT Null;
SELECT count() FROM l ANY LEFT JOIN r ON l.a = r.a
SETTINGS log_comment = '05327_join_engine_control' FORMAT Null;

-- A hash join that crosses `max_bytes_before_external_join` while it is filled switches to
-- `GraceHashJoin`. The parallel table it was filling is dropped unfinished, and each bucket is serial.
-- The number of buckets depends on the memory the table takes, so the test checks only that it
-- builds a serial table.
SELECT count() FROM numbers(100000) AS x JOIN (SELECT number AS a FROM numbers(100000)) AS y ON x.number = y.a
SETTINGS max_bytes_before_external_join = 1000000, log_comment = '05327_spill_switched' FORMAT Null;
SELECT count() FROM numbers(100000) AS x JOIN (SELECT number AS a FROM numbers(100000)) AS y ON x.number = y.a
SETTINGS max_bytes_before_external_join = 10000000000, log_comment = '05327_spill_in_memory' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- clickhouse-test sets `log_comment` to the file name for the other queries, and it also starts with `05327_`.
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND match(log_comment, '^05327_[a-z_]+$') AND NOT startsWith(log_comment, '05327_spill_')
ORDER BY event_time_microseconds;

SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] > 0 AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS switched
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05327_spill_')
ORDER BY event_time_microseconds;

DROP TABLE l;
DROP TABLE r;
DROP TABLE sl;
DROP TABLE sr;
DROP TABLE tj;
