-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

-- Counting from partition values or implicit projections would bypass the reads
-- we want to observe.
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS test_part_prune;
DROP TABLE IF EXISTS test_part_modulo;

CREATE TABLE test_part_prune (ts DateTime('UTC'), x UInt32) ENGINE = MergeTree
PARTITION BY (toYYYYMM(ts), toDate(ts))
ORDER BY x
SETTINGS index_granularity = 1;

-- 6 days spanning a month boundary, 4 rows per day: 6 partitions.
INSERT INTO test_part_prune SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR, number FROM numbers(24);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_part_prune' AND active;

-- Point predicate on ts prunes partitions via both partition subexpressions.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE ts = toDateTime('2026-03-31 06:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE ts = toDateTime('2026-03-31 06:00:00', 'UTC');
SELECT count() FROM test_part_prune WHERE ts = toDateTime('2026-03-31 06:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Range predicate on ts.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE ts >= toDateTime('2026-04-02 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE ts >= toDateTime('2026-04-02 00:00:00', 'UTC');
SELECT count() FROM test_part_prune WHERE ts >= toDateTime('2026-04-02 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- IN over ts.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'));
SELECT count() FROM test_part_prune WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Direct predicate on a partition subexpression.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE toDate(ts) = toDate('2026-03-31')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE toDate(ts) = toDate('2026-03-31');
SELECT count() FROM test_part_prune WHERE toDate(ts) = toDate('2026-03-31') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Negated predicates must not drop partitions containing other matching rows.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE ts != toDateTime('2026-03-31 06:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE ts != toDateTime('2026-03-31 06:00:00', 'UTC');
SELECT count() FROM test_part_prune WHERE ts != toDateTime('2026-03-31 06:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_prune WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_prune WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'));
SELECT count() FROM test_part_prune WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- moduloLegacy in the partition key with modulo predicates.
CREATE TABLE test_part_modulo (k UInt64) ENGINE = MergeTree
PARTITION BY moduloLegacy(k, 10)
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO test_part_modulo SELECT number FROM numbers(30);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_modulo WHERE k % 10 = 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_modulo WHERE k % 10 = 3;
SELECT count() FROM test_part_modulo WHERE k % 10 = 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_part_modulo WHERE k = 13) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_part_modulo WHERE k = 13 SETTINGS force_primary_key = 1;
SELECT count() FROM test_part_modulo WHERE k = 13 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_part_prune;
DROP TABLE test_part_modulo;
