-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_mt_dt;

CREATE TABLE test_mt_dt (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_mt_dt SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24 * 40);

-- Equality: one predicate constrains all three key columns.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts = toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts = toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- Strict and non-strict ranges.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts < toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts < toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts <= toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts <= toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts > toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts > toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts >= toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts >= toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts BETWEEN toDateTime('2026-01-10 00:00:00', 'UTC') AND toDateTime('2026-01-11 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts BETWEEN toDateTime('2026-01-10 00:00:00', 'UTC') AND toDateTime('2026-01-11 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- IN with values spanning two months.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-01-10 01:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-01-10 01:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- has(const_array, key).
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE has([toDateTime('2026-01-10 05:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE has([toDateTime('2026-01-10 05:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')], ts) SETTINGS force_primary_key = 1;

-- Conjunction of ranges.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts >= toDateTime('2026-01-10 00:00:00', 'UTC') AND ts < toDateTime('2026-01-11 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts >= toDateTime('2026-01-10 00:00:00', 'UTC') AND ts < toDateTime('2026-01-11 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- Disjunction of ranges.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts < toDateTime('2026-01-03 00:00:00', 'UTC') OR ts >= toDateTime('2026-02-08 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts < toDateTime('2026-01-03 00:00:00', 'UTC') OR ts >= toDateTime('2026-02-08 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- NOT over a range is rewritten by inversion push-down.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE NOT (ts < toDateTime('2026-01-10 05:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE NOT (ts < toDateTime('2026-01-10 05:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- Nested AND of OR.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE (ts < toDateTime('2026-01-02 00:00:00', 'UTC') OR ts >= toDateTime('2026-02-09 00:00:00', 'UTC')) AND ts != toDateTime('2026-01-01 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE (ts < toDateTime('2026-01-02 00:00:00', 'UTC') OR ts >= toDateTime('2026-02-09 00:00:00', 'UTC')) AND ts != toDateTime('2026-01-01 05:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- indexHint prunes reads but does not filter rows, so the result reflects whole granules
-- that may contain the value; no ground-truth comparison is applicable.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT min(ts), max(ts) FROM test_mt_dt WHERE indexHint(ts = toDateTime('2026-01-10 05:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT min(ts), max(ts) FROM test_mt_dt WHERE indexHint(ts = toDateTime('2026-01-10 05:00:00', 'UTC'));

-- Constant folds.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE 0 AND ts = toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE 0 AND ts = toDateTime('2026-01-10 05:00:00', 'UTC');
SELECT count() FROM test_mt_dt WHERE 0 AND ts = toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE 1 OR ts = toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE 1 OR ts = toDateTime('2026-01-10 05:00:00', 'UTC');
SELECT count() FROM test_mt_dt WHERE 1 OR ts = toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE NULL AND ts = toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE NULL AND ts = toDateTime('2026-01-10 05:00:00', 'UTC');
SELECT count() FROM test_mt_dt WHERE NULL AND ts = toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Negated equality: the direct key column atom is exact and may prune; rows sharing
-- toDate/toYYYYMM with the constant must survive.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts != toDateTime('2026-01-10 05:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts != toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT count() FROM test_mt_dt WHERE ts != toDateTime('2026-01-10 05:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Negated IN with a multi-element set.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_mt_dt WHERE ts NOT IN (toDateTime('2026-01-10 05:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_mt_dt WHERE ts NOT IN (toDateTime('2026-01-10 05:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;
SELECT count() FROM test_mt_dt WHERE ts NOT IN (toDateTime('2026-01-10 05:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_mt_dt;
