-- An outer `LIMIT` closes every output of the repartitioning stage of `aggregation_in_order_shuffle` as soon
-- as it is satisfied - the same thing a cancellation does. That can land while `BufferedShardByHashTransform`
-- is already splitting a block, so the split has to stop building per-shard copies nobody will consume, free
-- the copies it built for the closed outputs, and let the query finish instead of failing on a buffer budget
-- that only the abandoned data crossed.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle for the whole test, so reset it.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_outer_limit;

CREATE TABLE t_aio_shuffle_outer_limit (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Single-key parts: the worst case for buffering (see 04515), so the shuffle is forced to read far ahead and
-- the query hits its outer `LIMIT` long before the input is exhausted.
SYSTEM STOP MERGES t_aio_shuffle_outer_limit;
INSERT INTO t_aio_shuffle_outer_limit SELECT 1, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 2, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 3, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 4, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 5, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 6, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 7, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT 8, number FROM numbers(500000);

-- The same query without the `LIMIT` fails on this budget (04515). With the `LIMIT` the outputs are closed
-- while the split is in flight, so it must complete: exactly one row, with the right aggregate. No `ORDER BY`
-- (it would plan a sorted merge instead of the shuffle), so which key is returned is arbitrary - but every key
-- holds the same values, so the aggregate is not.
SELECT count() FROM (
    SELECT k, sum(v) AS s FROM t_aio_shuffle_outer_limit GROUP BY k LIMIT 1
    SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
             aggregation_in_order_shuffle_max_buffered_bytes = 1
) WHERE s = 124999750000;

DROP TABLE t_aio_shuffle_outer_limit;
