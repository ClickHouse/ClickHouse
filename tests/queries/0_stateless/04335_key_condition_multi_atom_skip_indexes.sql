-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_skip_mm;
DROP TABLE IF EXISTS test_skip_disj;
DROP TABLE IF EXISTS test_skip_set;
DROP TABLE IF EXISTS test_skip_final;

-- Multi-expression minmax skip index: one predicate on ts constrains both index columns.
CREATE TABLE test_skip_mm (id UInt32, ts DateTime('UTC'), INDEX mm (toDate(ts), ts) TYPE minmax GRANULARITY 1) ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_mm SELECT number, toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_mm WHERE ts = toDateTime('2026-03-02 06:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Name%';
SELECT count() FROM test_skip_mm WHERE ts = toDateTime('2026-03-02 06:00:00', 'UTC') SETTINGS force_data_skipping_indices = 'mm';
SELECT count() FROM test_skip_mm WHERE ts = toDateTime('2026-03-02 06:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_mm WHERE ts > toDateTime('2026-03-04 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Name%';
SELECT count() FROM test_skip_mm WHERE ts > toDateTime('2026-03-04 00:00:00', 'UTC') SETTINGS force_data_skipping_indices = 'mm';
SELECT count() FROM test_skip_mm WHERE ts > toDateTime('2026-03-04 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Disjunctions over two indexed columns with multi-atom expansion per branch.
-- The partial-disjunction machinery is limited to RPNs of 32 elements; exercise both
-- below the limit and above it (where it must disable itself gracefully).
CREATE TABLE test_skip_disj (id UInt32, ta DateTime('UTC'), tb DateTime('UTC'),
    INDEX ia (toDate(ta), ta) TYPE minmax GRANULARITY 1,
    INDEX ib (toDate(tb), tb) TYPE minmax GRANULARITY 1) ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_disj SELECT number, toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL number HOUR, toDateTime('2026-06-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(48);

-- Small disjunction (within the 32-element template limit).
SELECT count() FROM test_skip_disj WHERE ta = toDateTime('2026-03-01 05:00:00', 'UTC') OR tb = toDateTime('2026-06-02 07:00:00', 'UTC') OR ta = toDateTime('2026-03-02 11:00:00', 'UTC') SETTINGS use_skip_indexes_for_disjunctions = 1;
SELECT count() FROM test_skip_disj WHERE ta = toDateTime('2026-03-01 05:00:00', 'UTC') OR tb = toDateTime('2026-06-02 07:00:00', 'UTC') OR ta = toDateTime('2026-03-02 11:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Wide disjunction (the multi-atom RPN exceeds the 32-element template limit).
SELECT count() FROM test_skip_disj WHERE ta = toDateTime('2026-03-01 01:00:00', 'UTC') OR tb = toDateTime('2026-06-01 02:00:00', 'UTC') OR ta = toDateTime('2026-03-01 03:00:00', 'UTC') OR tb = toDateTime('2026-06-01 04:00:00', 'UTC') OR ta = toDateTime('2026-03-01 05:00:00', 'UTC') OR tb = toDateTime('2026-06-01 06:00:00', 'UTC') OR ta = toDateTime('2026-03-01 07:00:00', 'UTC') OR tb = toDateTime('2026-06-01 08:00:00', 'UTC') OR ta = toDateTime('2026-03-01 09:00:00', 'UTC') OR tb = toDateTime('2026-06-01 10:00:00', 'UTC') OR ta = toDateTime('2026-03-01 11:00:00', 'UTC') OR tb = toDateTime('2026-06-01 12:00:00', 'UTC') SETTINGS use_skip_indexes_for_disjunctions = 1;
SELECT count() FROM test_skip_disj WHERE ta = toDateTime('2026-03-01 01:00:00', 'UTC') OR tb = toDateTime('2026-06-01 02:00:00', 'UTC') OR ta = toDateTime('2026-03-01 03:00:00', 'UTC') OR tb = toDateTime('2026-06-01 04:00:00', 'UTC') OR ta = toDateTime('2026-03-01 05:00:00', 'UTC') OR tb = toDateTime('2026-06-01 06:00:00', 'UTC') OR ta = toDateTime('2026-03-01 07:00:00', 'UTC') OR tb = toDateTime('2026-06-01 08:00:00', 'UTC') OR ta = toDateTime('2026-03-01 09:00:00', 'UTC') OR tb = toDateTime('2026-06-01 10:00:00', 'UTC') OR ta = toDateTime('2026-03-01 11:00:00', 'UTC') OR tb = toDateTime('2026-06-01 12:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Set skip index goes through the single-atom adapter of RPNBuilder.
CREATE TABLE test_skip_set (id UInt32, s String, INDEX st (s) TYPE set(10) GRANULARITY 1) ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_set SELECT number, char(97 + number % 8) FROM numbers(32);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_set WHERE s = 'c') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Name%';
SELECT count() FROM test_skip_set WHERE s = 'c' SETTINGS force_data_skipping_indices = 'st';
SELECT count() FROM test_skip_set WHERE s = 'c' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- FINAL with skip indexes in exact mode.
CREATE TABLE test_skip_final (id UInt32, ts DateTime('UTC'), INDEX mm (toDate(ts), ts) TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_final SELECT number, toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);
INSERT INTO test_skip_final SELECT number, toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

SELECT count() FROM test_skip_final FINAL WHERE ts > toDateTime('2026-03-04 00:00:00', 'UTC') SETTINGS use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;
SELECT count() FROM test_skip_final FINAL WHERE ts > toDateTime('2026-03-04 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_skip_mm;
DROP TABLE test_skip_disj;
DROP TABLE test_skip_set;
DROP TABLE test_skip_final;
