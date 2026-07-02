-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for issue #107946: aggregating over a Merge table whose children are Distributed
-- tables raised the LOGICAL_ERROR exception 'ScatterExchangeStep should have one source shard, got 8'.
-- StorageMerge builds its children at a post-FetchColumns stage and converts them to distributed plans
-- on their own, so the per-child plan carries a ScatterExchange above an already-bucketed distributed
-- read. The scatter then sees several source buckets, which it must repartition as a plain shuffle
-- rather than reject. The query must run instead of throwing.

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

-- max_rows_to_group_by must be 0: the CI test profile sets it to 10G, and make_distributed_plan
-- rejects aggregation with a non-zero limit (Code 344), which would mask the multi-source scatter path.
-- prefer_localhost_replica must be 1: with 0 the Distributed engine serializes the plan to the
-- localhost replica, where the experimental BlocksMarshalling step is not deserializable (Code 47);
-- that path is unrelated to this issue, which happens earlier while building the per-child plan.
-- distributed_plan_max_rows_to_broadcast must be 0 and distributed_plan_default_reader_bucket_count > 1:
-- otherwise the distributed read could be broadcast or use a single bucket, so the scatter source would
-- have one bucket and the removed guard would never be exercised. 3 forces a multi-bucket source.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    use_statistics = 1, distributed_plan_optimize_exchanges = 1, enable_join_runtime_filters = 0,
    max_rows_to_group_by = 0, prefer_localhost_replica = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3;

-- The exact reproducer from the issue. The Merge child plans are distributed, so the scatter built for
-- the per-child aggregation sees the bucketed distributed read (3 source buckets) as its source. No rows
-- match because the Merge exposes its underlying table names (base107946_*) through _table, not the
-- Distributed names d107946_*. This proves the issue query no longer throws.
SELECT count(_table) FROM m107946 WHERE _table = 'd107946_1' GROUP BY _table;

-- Same Merge-over-Distributed topology and the same single-stage outer plan, but filtering on a real
-- underlying table name so rows survive the multi-bucket scatter. The counts must match the per-table
-- row counts, proving the scatter repartitioned the data correctly rather than just not throwing.
SELECT count(_table) FROM m107946 WHERE _table = 'base107946_1' GROUP BY _table;
SELECT count(_table) FROM m107946 WHERE _table = 'base107946_4' GROUP BY _table;

DROP TABLE m107946;
DROP TABLE d107946_1;
DROP TABLE d107946_4;
DROP TABLE base107946_1;
DROP TABLE base107946_4;
