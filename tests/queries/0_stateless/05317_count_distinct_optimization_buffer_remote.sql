-- Tags: distributed

-- `count_distinct_optimization` must not break reading from a `Buffer` table whose destination
-- is remote and which still holds buffered rows. The rewrite is skipped at the top level because
-- the `Buffer` reports itself as remote, and the plan built for the in-memory buffers must keep
-- the same result shape as the destination plan: a buffers plan producing `count()` while the
-- destination plan produces an `AggregateFunction(uniqExact, ...)` state would throw
-- `CANNOT_CONVERT_TYPE`. Two shards make the destination return the mergeable state.

DROP TABLE IF EXISTS t_buf;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_mt;

CREATE TABLE t_mt (c0 Int) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_mt SELECT * FROM numbers(10);

CREATE TABLE t_dist (c0 Int) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 't_mt');

-- Thresholds are large enough that the inserted rows stay in the buffer.
CREATE TABLE t_buf (c0 Int) ENGINE = Buffer(currentDatabase(), 't_dist', 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_buf SELECT number + 5 FROM numbers(10);

SET count_distinct_optimization = 1;

-- { echoOn }
SELECT uniqExact(c0) FROM t_buf;
SELECT countDistinct(c0) FROM t_buf;
SELECT uniqExact(c0) FROM t_buf SETTINGS count_distinct_optimization = 0;
-- { echoOff }

DROP TABLE t_buf;
DROP TABLE t_dist;
DROP TABLE t_mt;
