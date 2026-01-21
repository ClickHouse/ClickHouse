SET enable_analyzer = 1;

-- Test special columns handling in ReadFromMergeTree::removeUnusedColumns with FINAL and PREWHERE
-- Similar to https://github.com/ClickHouse/ClickHouse/issues/45804

-- Test 1: ReplacingMergeTree with version column
DROP TABLE IF EXISTS test_replacing_mt;
CREATE TABLE test_replacing_mt(
  key Int64,
  someCol String,
  ver DateTime
) ENGINE = ReplacingMergeTree(ver)
ORDER BY key;

INSERT INTO test_replacing_mt VALUES
  (1, 'test1', '2020-01-01'),
  (1, 'test2', '2021-01-01'),
  (2, 'test3', '2020-06-01'),
  (2, 'test4', '2021-06-01');

-- Use version column in PREWHERE but not in SELECT
SELECT count() FROM test_replacing_mt FINAL PREWHERE ver > '2020-01-01';

-- Use version column in both PREWHERE and SELECT
SELECT count(ver) FROM test_replacing_mt FINAL PREWHERE ver > '2020-01-01';

DROP TABLE test_replacing_mt;

-- Test 2: ReplacingMergeTree with both version and is_deleted columns
DROP TABLE IF EXISTS test_replacing_mt_with_is_deleted;
CREATE TABLE test_replacing_mt_with_is_deleted(
  key Int64,
  someCol String,
  ver UInt64,
  is_deleted UInt8
) ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY key;

INSERT INTO test_replacing_mt_with_is_deleted VALUES
  (1, 'test1', 1, 0),
  (1, 'test2', 2, 0),
  (2, 'test3', 1, 0),
  (2, 'test4', 2, 1),
  (3, 'test5', 1, 1);

-- Use version column in PREWHERE but not in SELECT
SELECT count() FROM test_replacing_mt_with_is_deleted FINAL PREWHERE ver > 0;

-- Use is_deleted column in PREWHERE but not in SELECT
SELECT count() FROM test_replacing_mt_with_is_deleted FINAL PREWHERE is_deleted = 0;

-- Use version in PREWHERE and is_deleted in SELECT
SELECT count(is_deleted) FROM test_replacing_mt_with_is_deleted FINAL PREWHERE ver > 1;

-- Use one special column in PREWHERE and both in SELECT
SELECT ver, is_deleted FROM test_replacing_mt_with_is_deleted FINAL PREWHERE ver > 0 ORDER BY key;

DROP TABLE test_replacing_mt_with_is_deleted;

-- Test 3: CollapsingMergeTree with sign column
DROP TABLE IF EXISTS test_collapsing_mt;
CREATE TABLE test_collapsing_mt(
  key Int64,
  someCol String,
  sign Int8
) ENGINE = CollapsingMergeTree(sign)
ORDER BY key;

INSERT INTO test_collapsing_mt VALUES
  (1, 'test1', 1),
  (1, 'test1', -1),
  (2, 'test2', 1),
  (3, 'test3', 1);

-- Use sign column in PREWHERE but not in SELECT
SELECT count() FROM test_collapsing_mt FINAL PREWHERE sign = 1;

-- Use sign column in both PREWHERE and SELECT
SELECT count(sign) FROM test_collapsing_mt FINAL PREWHERE sign = 1;

DROP TABLE test_collapsing_mt;

-- Test 4: VersionedCollapsingMergeTree with both sign and version columns
DROP TABLE IF EXISTS test_versioned_collapsing_mt;
CREATE TABLE test_versioned_collapsing_mt(
  key Int64,
  someCol String,
  sign Int8,
  ver UInt64
) ENGINE = VersionedCollapsingMergeTree(sign, ver)
ORDER BY key;

INSERT INTO test_versioned_collapsing_mt VALUES
  (1, 'test1', 1, 1),
  (1, 'test1', -1, 1),
  (2, 'test2', 1, 2),
  (2, 'test2_updated', -1, 2),
  (2, 'test2_updated', 1, 3),
  (3, 'test3', 1, 1);

-- Use sign column in PREWHERE but not in SELECT
SELECT count() FROM test_versioned_collapsing_mt FINAL PREWHERE sign = 1;

-- Use version column in PREWHERE but not in SELECT
SELECT count() FROM test_versioned_collapsing_mt FINAL PREWHERE ver > 0;

-- Use both sign and version columns in PREWHERE but not in SELECT
SELECT count() FROM test_versioned_collapsing_mt FINAL PREWHERE sign = 1 AND ver > 1;

-- Use sign column in PREWHERE and SELECT
SELECT count(sign) FROM test_versioned_collapsing_mt FINAL PREWHERE sign = 1;

-- Use version column in PREWHERE and SELECT
SELECT count(ver) FROM test_versioned_collapsing_mt FINAL PREWHERE ver > 0;

DROP TABLE test_versioned_collapsing_mt;

-- Test 5: SummingMergeTree (no special columns, but test FINAL with PREWHERE)
DROP TABLE IF EXISTS test_summing_mt;
CREATE TABLE test_summing_mt(
  key Int64,
  value Int64,
  amount Int64
) ENGINE = SummingMergeTree(amount)
ORDER BY key;

INSERT INTO test_summing_mt VALUES
  (1, 10, 100),
  (1, 20, 200),
  (2, 30, 300),
  (2, 40, 400);

-- Use key column in PREWHERE
SELECT count() FROM test_summing_mt FINAL PREWHERE key > 1;

-- Use non-key column in PREWHERE
SELECT count() FROM test_summing_mt FINAL PREWHERE value > 15;

-- Use data columns in both PREWHERE and SELECT
SELECT key, sum(amount) as total FROM test_summing_mt FINAL PREWHERE value > 15 GROUP BY key ORDER BY key;

DROP TABLE test_summing_mt;

-- Test 6: AggregatingMergeTree (no special columns, but test FINAL with PREWHERE)
DROP TABLE IF EXISTS test_aggregating_mt;
CREATE TABLE test_aggregating_mt(
  key Int64,
  value Int64,
  agg_state AggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY key;

INSERT INTO test_aggregating_mt
SELECT
  key,
  value,
  initializeAggregation('sumState', amount::Int64) AS agg_state
FROM (
  SELECT intDiv(number, 2) + 1 AS key,
         (number + 1) * 10 AS value,
         (number + 1) * 100 AS amount
  FROM numbers(3)
);

-- Use key column in PREWHERE
SELECT count() FROM test_aggregating_mt FINAL PREWHERE key = 1;

-- Use non-key column in PREWHERE
SELECT count() FROM test_aggregating_mt FINAL PREWHERE value > 15;

-- Use data columns in both PREWHERE and SELECT
SELECT key FROM test_aggregating_mt FINAL PREWHERE value > 15 ORDER BY key;

DROP TABLE test_aggregating_mt;

-- Test 7: CoalescingMergeTree (no special columns, but test FINAL with PREWHERE)
DROP TABLE IF EXISTS test_coalescing_mt;
CREATE TABLE test_coalescing_mt(
  key Int64,
  value1 Nullable(Int64),
  value2 Nullable(String),
  value3 Nullable(Int64)
) ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_coalescing_mt VALUES
  (1, 10, 'first', NULL),
  (1, NULL, 'second', 100),
  (1, 30, NULL, NULL),
  (2, 40, 'test', 200),
  (2, NULL, NULL, 300);

-- Use key column in PREWHERE
SELECT count() FROM test_coalescing_mt FINAL PREWHERE key > 1;

-- Use non-key column in PREWHERE
SELECT count() FROM test_coalescing_mt FINAL PREWHERE value1 > 15;

-- Use data columns in both PREWHERE and SELECT
SELECT key, value1, value2 FROM test_coalescing_mt FINAL PREWHERE value3 IS NOT NULL ORDER BY key;

-- Use multiple columns in PREWHERE
SELECT key FROM test_coalescing_mt FINAL PREWHERE value1 IS NOT NULL AND value2 IS NOT NULL ORDER BY key;

DROP TABLE test_coalescing_mt;
