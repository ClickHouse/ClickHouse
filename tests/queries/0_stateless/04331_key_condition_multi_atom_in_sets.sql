-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_in_main;
DROP TABLE IF EXISTS test_in_wrapped;
DROP TABLE IF EXISTS test_in_uint8;
DROP TABLE IF EXISTS test_in_nullable;
DROP TABLE IF EXISTS test_in_packed;
DROP TABLE IF EXISTS test_in_lhs_tuple;

CREATE TABLE test_in_main (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

-- 6 days spanning a month boundary, 4 rows per day.
INSERT INTO test_in_main SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

-- Literal IN list.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- Subquery set.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- Set elements collapsing to one value after the toDate transform.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-03-31 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-03-31 12:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- globalIn behaves like IN on a local table.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts GLOBAL IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts GLOBAL IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS force_primary_key = 1;

-- has(const_array, key).
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE has([toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')], ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE has([toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')], ts) SETTINGS force_primary_key = 1;

-- Empty sets on the multi-expression key.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0);
SELECT count() FROM test_in_main WHERE ts IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts NOT IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts NOT IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0);
SELECT count() FROM test_in_main WHERE ts NOT IN (SELECT toDateTime('2026-03-31 06:00:00', 'UTC') WHERE 0) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE has([], ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE has([], ts);
SELECT count() FROM test_in_main WHERE has([], ts) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- NOT IN on the multi-expression key.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_main WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_main WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'));
SELECT count() FROM test_in_main WHERE ts NOT IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- No direct key column for ts: only wrapped set atoms are possible.
CREATE TABLE test_in_wrapped (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toDate(ts), toYYYYMM(ts))
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_in_wrapped SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_wrapped WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_wrapped WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS force_primary_key = 1;
SELECT count() FROM test_in_wrapped WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Set elements that cannot be cast to the key type are dropped from the set.
CREATE TABLE test_in_uint8 (x UInt8) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_in_uint8 SELECT number FROM numbers(10);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_uint8 WHERE x IN (1, 10000000000)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_uint8 WHERE x IN (1, 10000000000) SETTINGS force_primary_key = 1;
SELECT count() FROM test_in_uint8 WHERE x IN (1, 10000000000) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- NULL in the set with a Nullable key.
CREATE TABLE test_in_nullable (x Nullable(UInt8)) ENGINE = MergeTree
ORDER BY x
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_in_nullable VALUES (1), (2), (NULL);

SET transform_null_in = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_nullable WHERE x IN (1, NULL)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_nullable WHERE x IN (1, NULL);
SELECT count() FROM test_in_nullable WHERE x IN (1, NULL) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SET transform_null_in = 0;

-- Packed Tuple-typed key column compared against an unpacked set.
CREATE TABLE test_in_packed (tup Tuple(UInt8, UInt8)) ENGINE = MergeTree
ORDER BY tup
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_in_packed VALUES ((1, 2)), ((3, 4)), ((5, 6));

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_packed WHERE tup IN ((1, 2), (3, 4))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_packed WHERE tup IN ((1, 2), (3, 4)) SETTINGS force_primary_key = 1;

-- LHS tuple where both elements are key columns.
CREATE TABLE test_in_lhs_tuple (a UInt8, b UInt8, c UInt8) ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_in_lhs_tuple SELECT intDiv(number, 4), number % 4, number % 4 FROM numbers(16);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_lhs_tuple WHERE (a, b) IN ((1, 2), (3, 0))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_lhs_tuple WHERE (a, b) IN ((1, 2), (3, 0)) SETTINGS force_primary_key = 1;

-- LHS tuple where only the first element is a key column (the second is plain data).
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_in_lhs_tuple WHERE (a, c) IN ((1, 2), (3, 0))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_in_lhs_tuple WHERE (a, c) IN ((1, 2), (3, 0));
SELECT count() FROM test_in_lhs_tuple WHERE (a, c) IN ((1, 2), (3, 0)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_in_main;
DROP TABLE test_in_wrapped;
DROP TABLE test_in_uint8;
DROP TABLE test_in_nullable;
DROP TABLE test_in_packed;
DROP TABLE test_in_lhs_tuple;
