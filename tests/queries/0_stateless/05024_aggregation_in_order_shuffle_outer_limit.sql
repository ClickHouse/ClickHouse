-- An outer `LIMIT` closes every output of the repartitioning stage of `aggregation_in_order_shuffle` as soon
-- as it is satisfied - the same thing a cancellation does. That can land while `BufferedShardByHashTransform`
-- still has blocks in flight, so the stage has to drop what nobody will consume and let the query finish. The
-- interleaving inside a split is covered deterministically by the `BufferedShardByHashTransform` unit tests;
-- this checks the end-to-end query through the shuffle.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle for the whole test, so reset it.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_outer_limit;

CREATE TABLE t_aio_shuffle_outer_limit (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Every part holds every key (500 rows of each of 1000 keys), so the shard merges complete groups early and
-- the outer `LIMIT` is reached long before the input is exhausted, with the rest of the input still in flight.
SYSTEM STOP MERGES t_aio_shuffle_outer_limit;
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);
INSERT INTO t_aio_shuffle_outer_limit SELECT intDiv(number, 500), 1 FROM numbers(500000);

SET max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;

-- The query really goes through the repartitioning stage.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) AS s FROM t_aio_shuffle_outer_limit GROUP BY k LIMIT 1);

-- Exactly one row, with a complete group. No `ORDER BY` (it would plan a sorted merge instead of the shuffle),
-- so which key is returned is arbitrary - but every key has the same rows, so the aggregate is not.
SELECT sum(v) AS s FROM t_aio_shuffle_outer_limit GROUP BY k LIMIT 1;

DROP TABLE t_aio_shuffle_outer_limit;
