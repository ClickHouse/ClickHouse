-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test: an explicit aggregate projection over a distributed read (make_distributed_plan)
-- must not be applied to the sharded read; the projection optimization declines for distributed reads,
-- otherwise the Union split exposes different shard lists and makeDistributedPlan aborts.

DROP TABLE IF EXISTS agg2;

CREATE TABLE agg2 (id1 UInt32, id2 UInt32) ENGINE = MergeTree ORDER BY id1 SETTINGS index_granularity = 16;

-- Mixed parts: the first batch has no projection, the second does, so a match would split the read
-- into a Union of (surviving parts, projection parts).
INSERT INTO agg2 SELECT number, number % 10 FROM numbers(2000);
ALTER TABLE agg2 ADD PROJECTION aggproj (SELECT id2, count() GROUP BY id2);
INSERT INTO agg2 SELECT number, number % 10 FROM numbers(2000);

-- Pin settings that randomized runs could flip and mask the regression.
SET max_rows_to_group_by = 0;
SET optimize_use_projections = 1;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 10;

SELECT '-- distributed aggregate over an aggregate-projected table does not abort';
SELECT id2, count() FROM agg2 GROUP BY id2 ORDER BY id2;

SELECT '-- matches the single-node result';
SELECT id2, count() FROM agg2 GROUP BY id2 ORDER BY id2 SETTINGS make_distributed_plan = 0;

DROP TABLE agg2;
