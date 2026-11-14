-- Tags: no-parallel-replicas
-- A STREAM read returns parts in commit order, not sorting-key order, so DISTINCT/aggregation/
-- LIMIT BY in order must not be applied to it. Before the fix the query plan optimizer requested
-- read-in-order on the streaming read, which advertised a sorting-key order that did not hold and
-- fed unsorted data to DistinctSortedStreamTransform, aborting with
-- "Equal values are not contiguous within the range assumed to be sorted".

SET enable_streaming_queries = 1;
SET max_threads = 1;

-- Pin the trigger settings for the query-plan DISTINCT-in-order pass that this PR guards.
-- CI randomizes these; with any of them off the query takes the generic distinct path and the
-- unfixed build never reaches the bad input_order_info, so the test would pass without the fix.
SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1;
SET optimize_distinct_in_order = 1;

DROP TABLE IF EXISTS t_stream_distinct_in_order;

CREATE TABLE t_stream_distinct_in_order (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

-- Several parts whose commit-order concatenation is not globally sorted by `a`
-- (each part is internally sorted, but the value ranges overlap across parts).
INSERT INTO t_stream_distinct_in_order SELECT toString(number % 100), number FROM numbers(5000);
INSERT INTO t_stream_distinct_in_order SELECT toString(number % 100), number FROM numbers(5000);
INSERT INTO t_stream_distinct_in_order SELECT toString(number % 100), number FROM numbers(5000);

-- DISTINCT in order over a streaming read. LIMIT is below the number of distinct values so the
-- query completes from storage. Must not crash; returns the requested number of rows.
SELECT count() FROM (SELECT DISTINCT a FROM t_stream_distinct_in_order STREAM LIMIT 50);
SELECT count() FROM (SELECT DISTINCT * FROM t_stream_distinct_in_order STREAM LIMIT 50);

DROP TABLE t_stream_distinct_in_order;
