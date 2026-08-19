-- The adaptive aggregation runs its staged chunks and its merge through the pipeline: every
-- producing `AggregatingTransform` feeds the `AdaptiveAggregationMerge` store, which assembles
-- the merge once every producer finished. These cells pin that wiring through `EXPLAIN
-- PIPELINE`, asserting only the presence or absence of the store (never counts or layout, which
-- depend on the machine).

-- Pinned against the test runner's settings randomization and the stateless limits profile:
-- the admission needs the feature on, a parallel pipeline, two-level aggregation enabled, and
-- no group-by limit (`users.d/limits.yaml` sets `max_rows_to_group_by`, which the admission
-- rejects); `max_block_size` is pinned because a large enough randomized value collapses the
-- numbers source into a single stream, which the admission also rejects.
SET max_rows_to_group_by = 0;
SET max_threads = 4;
SET enable_adaptive_aggregator = 1;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 500000000;
SET max_block_size = 65536;

-- An admitted parallel aggregation routes through exactly one store.
SELECT count() FROM (EXPLAIN PIPELINE SELECT intHash64(number) AS k, count() FROM numbers_mt(1000000) GROUP BY k)
WHERE explain LIKE '%AdaptiveAggregationMerge%';

-- With the feature off the store must not appear.
SELECT count() FROM (EXPLAIN PIPELINE SELECT intHash64(number) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
WHERE explain LIKE '%AdaptiveAggregationMerge%';

-- A single-stream aggregation is not admitted, so it keeps the plain shape.
SELECT count() FROM (EXPLAIN PIPELINE SELECT intHash64(number) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 1)
WHERE explain LIKE '%AdaptiveAggregationMerge%';

-- A non-admitted key type (8-bit, fixed hash map) keeps the plain shape too.
SELECT count() FROM (EXPLAIN PIPELINE SELECT toUInt8(number % 10) AS k, count() FROM numbers_mt(1000000) GROUP BY k)
WHERE explain LIKE '%AdaptiveAggregationMerge%';
