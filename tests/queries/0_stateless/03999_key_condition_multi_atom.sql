-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_multi_atom;

CREATE TABLE test_multi_atom (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_multi_atom SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24 * 40);

-- A range on ts constrains all three key columns.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE ts >= toDateTime('2026-01-10 00:00:00', 'UTC') AND ts < toDateTime('2026-01-11 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE ts >= toDateTime('2026-01-10 00:00:00', 'UTC') AND ts < toDateTime('2026-01-11 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- IN over ts with values spanning two months.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-01-10 01:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-01-10 01:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- has(const_array, ts).
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE has([toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE has([toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts) SETTINGS force_primary_key = 1;

-- Direct IN over a key expression.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE toDate(ts) IN (toDate('2026-01-10'), toDate('2026-02-05'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE toDate(ts) IN (toDate('2026-01-10'), toDate('2026-02-05')) SETTINGS force_primary_key = 1;

-- IN with Date-typed constants against the DateTime column.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05')) SETTINGS force_primary_key = 1;
SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Disjunction mixing the transformed IN, a range, has and an equality.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05')) OR ts <= toDateTime('2026-01-10 00:00:00', 'UTC') OR has([toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts) OR ts = toDateTime('2026-01-10 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05')) OR ts <= toDateTime('2026-01-10 00:00:00', 'UTC') OR has([toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts) OR ts = toDateTime('2026-01-10 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT count() FROM test_multi_atom WHERE ts IN (toDate('2026-01-10'), toDate('2026-02-05')) OR ts <= toDateTime('2026-01-10 00:00:00', 'UTC') OR has([toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts) OR ts = toDateTime('2026-01-10 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_multi_atom;
