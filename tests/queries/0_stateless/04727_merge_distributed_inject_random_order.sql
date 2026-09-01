-- Tags: shard
-- With `inject_random_order_for_select_without_order_by` enabled, an aggregation over a Merge table
-- that contains a Distributed child used to fail with a logical error (exception)
-- "Chunk info was not set for chunk in MergingAggregatedTransform": the injected `ORDER BY rand()` wrapper
-- was applied to child plans processed only up to `WithMergeableState`, so they returned fully aggregated
-- blocks where partially aggregated blocks were expected.

DROP TABLE IF EXISTS t_local_inject;
DROP TABLE IF EXISTS t_dist_inject;

CREATE TABLE t_local_inject (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local_inject SELECT number FROM numbers(100);
CREATE TABLE t_dist_inject (x UInt64) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_local_inject');

SET inject_random_order_for_select_without_order_by = 1;

SELECT count() FROM merge(currentDatabase(), '^t_(local|dist)_inject$');
SELECT count() FROM merge(currentDatabase(), '^t_(local|dist)_inject$') SETTINGS distributed_aggregation_memory_efficient = 0;
SELECT sum(x) FROM merge(currentDatabase(), '^t_(local|dist)_inject$') GROUP BY x % 2 ORDER BY 1;

DROP TABLE t_dist_inject;
DROP TABLE t_local_inject;
