-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- The implicit count/minmax projection counts rows from part metadata. A distributed read buckets the
-- part across workers; if the projection is left enabled it is replicated to every bucket and counts
-- the whole part each time, so the result is multiplied by the bucket count. These distributed counts
-- must match a single-node read.

DROP TABLE IF EXISTS t_one;
DROP TABLE IF EXISTS t_cnt;
CREATE TABLE t_one (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_cnt (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_one SELECT number FROM numbers(200000);
INSERT INTO t_cnt SELECT number, number FROM numbers(200000);

-- Pin the implicit projection on so the bug path is exercised; the fix disables it for distributed
-- plans regardless. Without pinning, randomized settings can turn it off and mask the regression.
-- Pin max_rows_to_group_by to 0 too: distributed aggregation rejects a nonzero limit (randomized).
SET max_rows_to_group_by = 0;

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    optimize_use_implicit_projections = 1;

-- Trivial count over a distributed read (counted from part metadata).
SELECT count() FROM (SELECT x FROM t_one);
-- Primary-key range: full granules counted from the index, only the boundary granule scanned.
SELECT count() FROM t_cnt WHERE k > 100000;
-- Non-index filter: a real per-row scan (always correct; guards against regression the other way).
SELECT count() FROM t_cnt WHERE v > 100000;

DROP TABLE t_one;
DROP TABLE t_cnt;
