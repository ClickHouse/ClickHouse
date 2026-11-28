-- Regression test for issue #116849: with `optimize_aggregation_in_order_limit`
-- enabled, a query whose `ORDER BY` is a strict prefix of the `GROUP BY` key
-- returned incomplete aggregate values when groups tie on that prefix and a
-- group's rows span more than one part. Each in-order stream stopped as soon as
-- it had emitted `LIMIT` groups, at a boundary of the full group key; now it
-- stops only at a boundary of the `ORDER BY` prefix, so the groups it drops
-- always sort after at least `LIMIT` complete groups.

DROP TABLE IF EXISTS t_agg_in_order_limit_prefix;

CREATE TABLE t_agg_in_order_limit_prefix (a UInt32, b UInt32, x UInt32)
ENGINE = MergeTree ORDER BY (a, b);

SYSTEM STOP MERGES t_agg_in_order_limit_prefix;

-- Two parts; groups with b in 4..20 span both parts.
INSERT INTO t_agg_in_order_limit_prefix SELECT 1, number, 10 FROM numbers(1, 20);
INSERT INTO t_agg_in_order_limit_prefix SELECT 1, number, 1 FROM numbers(4, 17);

-- Ground truth: sum(x) is 10 for groups with b in 1..3 and 11 for groups with b in 4..20.
-- All groups tie on `a`, so any three groups are a legal answer; assert that every
-- returned group carries its complete aggregate value. The check is done in the
-- projection on purpose: wrapping the query into a subquery changes the plan
-- (the aggregation is no longer executed in order), which hides the bug. The block
-- settings are pinned because tiny blocks also hide it, and `max_threads` because with a
-- single thread and `read_in_order_two_level_merge_threshold` at most the number of parts
-- the parts are merged into one stream before the aggregation, where the bug cannot occur.
SELECT sum(x) = if(b <= 3, 10, 11)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a
LIMIT 3
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

-- Same with OFFSET.
SELECT sum(x) = if(b <= 3, 10, 11)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a
LIMIT 3 OFFSET 2
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

-- The full-key `ORDER BY` still admits the push-down and stays correct.
SELECT a, b, sum(x)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a, b
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

DROP TABLE t_agg_in_order_limit_prefix;

-- With enough distinct values of the prefix the push-down still stops the streams
-- early, and the groups it returns are complete even though every group spans two parts.

DROP TABLE IF EXISTS t_agg_in_order_limit_prefix_reads;

CREATE TABLE t_agg_in_order_limit_prefix_reads (a UInt32, b UInt32, x UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_agg_in_order_limit_prefix_reads;

-- Two parts, 100 values of `a` with 10 values of `b` each; sum(x) is 3 for every group.
INSERT INTO t_agg_in_order_limit_prefix_reads SELECT intDiv(number, 10), number % 10, 1 FROM numbers(1000);
INSERT INTO t_agg_in_order_limit_prefix_reads SELECT intDiv(number, 10), number % 10, 2 FROM numbers(1000);

-- Every returned group must be complete.
SELECT sum(x) = 3
FROM t_agg_in_order_limit_prefix_reads
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

SELECT sum(x) = 3
FROM t_agg_in_order_limit_prefix_reads
GROUP BY a, b
ORDER BY a
LIMIT 5 OFFSET 7
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

-- The push-down is observed through `read_rows`. The small-block settings expose the
-- effect on a 2000-row table; `enable_parallel_replicas = 0` is required because
-- `read_rows` is accounted per reading node, and `read_in_order_two_level_merge_threshold`
-- keeps the two parts in separate streams instead of merging them before the aggregation.
SELECT sum(x) = 3
FROM t_agg_in_order_limit_prefix_reads
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 1, max_block_size = 16, read_in_order_two_level_merge_threshold = 100,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0,
         merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '05055_prefix_pushdown_on';

SELECT sum(x) = 3
FROM t_agg_in_order_limit_prefix_reads
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_threads = 1, max_block_size = 16, read_in_order_two_level_merge_threshold = 100,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0,
         merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '05055_prefix_pushdown_off';

SYSTEM FLUSH LOGS query_log;

SELECT if(on_reads < off_reads, 'PUSHDOWN_FIRES', format('FAIL: on={} off={}', on_reads, off_reads))
FROM
(
    SELECT
        anyIf(read_rows, log_comment = '05055_prefix_pushdown_on') AS on_reads,
        anyIf(read_rows, log_comment = '05055_prefix_pushdown_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('05055_prefix_pushdown_on', '05055_prefix_pushdown_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE t_agg_in_order_limit_prefix_reads;

-- The table is sorted only by `a`, so the in-order aggregation of `GROUP BY a, b` keeps the
-- groups of one `a` run in a hash table (the `group_by_key` path of `AggregatingInOrderTransform`).
-- The stream must count the groups it accumulated, not the `a` runs: a single run already
-- holds 10 complete groups, so a `LIMIT 5` stream stops right after the first run.

DROP TABLE IF EXISTS t_agg_in_order_limit_partial_key;

CREATE TABLE t_agg_in_order_limit_partial_key (a UInt32, b UInt32, x UInt32)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_agg_in_order_limit_partial_key;

-- Two parts, 100 values of `a` with 10 values of `b` each; sum(x) is 3 for every group.
INSERT INTO t_agg_in_order_limit_partial_key SELECT intDiv(number, 10), number % 10, 1 FROM numbers(1000);
INSERT INTO t_agg_in_order_limit_partial_key SELECT intDiv(number, 10), number % 10, 2 FROM numbers(1000);

SELECT sum(x) = 3
FROM t_agg_in_order_limit_partial_key
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

SELECT sum(x) = 3
FROM t_agg_in_order_limit_partial_key
GROUP BY a, b
ORDER BY a, b
LIMIT 5 OFFSET 7
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 2, max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

SELECT sum(x) = 3
FROM t_agg_in_order_limit_partial_key
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 1, max_block_size = 16, read_in_order_two_level_merge_threshold = 100,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0,
         merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '05055_partial_key_pushdown_on';

SELECT sum(x) = 3
FROM t_agg_in_order_limit_partial_key
GROUP BY a, b
ORDER BY a
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_threads = 1, max_block_size = 16, read_in_order_two_level_merge_threshold = 100,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0,
         merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '05055_partial_key_pushdown_off';

SYSTEM FLUSH LOGS query_log;

-- Stopping after the first `a` run reads a small fraction of the table; counting `a` runs
-- instead of groups would need five runs, i.e. several times more rows.
SELECT if(on_reads * 20 <= off_reads, 'PUSHDOWN_FIRES', format('FAIL: on={} off={}', on_reads, off_reads))
FROM
(
    SELECT
        anyIf(read_rows, log_comment = '05055_partial_key_pushdown_on') AS on_reads,
        anyIf(read_rows, log_comment = '05055_partial_key_pushdown_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('05055_partial_key_pushdown_on', '05055_partial_key_pushdown_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE t_agg_in_order_limit_partial_key;
