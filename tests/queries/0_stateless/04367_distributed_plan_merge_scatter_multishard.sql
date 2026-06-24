-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for issue #107946: aggregating over a Merge table whose children are Distributed
-- tables crashed the server with LOGICAL_ERROR 'ScatterExchangeStep should have one source shard, got 8'.
-- StorageMerge builds its children at a post-FetchColumns stage and converts them to distributed plans
-- on their own, so the per-child plan carries a ScatterExchange above an already-bucketed distributed
-- read. The scatter then sees several source buckets, which it must repartition as a plain shuffle
-- rather than reject. The query must run (and survive) instead of aborting.

DROP TABLE IF EXISTS m107946;
DROP TABLE IF EXISTS d107946_1;
DROP TABLE IF EXISTS d107946_4;
DROP TABLE IF EXISTS base107946_1;
DROP TABLE IF EXISTS base107946_4;

CREATE TABLE base107946_1 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE base107946_4 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO base107946_1 SELECT number, number FROM numbers(100000);
INSERT INTO base107946_4 SELECT number, number FROM numbers(50000);

CREATE TABLE d107946_1 AS base107946_1 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base107946_1);
CREATE TABLE d107946_4 AS base107946_4 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base107946_4);
CREATE TABLE m107946 ENGINE = Merge(currentDatabase(), '^d107946_(1|4)$');

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    use_statistics = 1, distributed_plan_optimize_exchanges = 1, enable_join_runtime_filters = 0;

-- The exact reproducer from the issue. The Merge child plans are distributed, so the scatter built for
-- the per-child aggregation sees the bucketed distributed read as its source. No rows match (the
-- underlying tables are named base107946_*, not d107946_*), which is the same result a single-node plan
-- produces - the point is that the server must not crash.
SELECT count(_table) FROM m107946 WHERE _table = 'd107946_1' GROUP BY _table;

SELECT 'survived';

DROP TABLE m107946;
DROP TABLE d107946_1;
DROP TABLE d107946_4;
DROP TABLE base107946_1;
DROP TABLE base107946_4;
