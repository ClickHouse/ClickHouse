-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

-- Pruning-power floor: these are the classic single-atom scenarios; the pinned Granules
-- counts must keep showing real pruning so that it can not be lost silently.

DROP TABLE IF EXISTS test_floor_main;
DROP TABLE IF EXISTS test_floor_wrap;
DROP TABLE IF EXISTS test_floor_concat;
DROP TABLE IF EXISTS test_floor_direct;
DROP TABLE IF EXISTS test_floor_nullable;
DROP TABLE IF EXISTS test_floor_str;

CREATE TABLE test_floor_main (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_main SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

-- Direct predicates on key expressions.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_main WHERE toDate(ts) = toDate('2026-03-31')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_main WHERE toDate(ts) = toDate('2026-03-31') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_main WHERE toYYYYMM(ts) = 202604) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_main WHERE toYYYYMM(ts) = 202604 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_main WHERE toDate(ts) IN (toDate('2026-03-31'), toDate('2026-04-02'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_main WHERE toDate(ts) IN (toDate('2026-03-31'), toDate('2026-04-02')) SETTINGS force_primary_key = 1;

-- Monotonic constant wrapping.
CREATE TABLE test_floor_wrap (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY toDate(ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_wrap SELECT toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_wrap WHERE ts > toDateTime('2026-03-04 06:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_wrap WHERE ts > toDateTime('2026-03-04 06:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT count() FROM test_floor_wrap WHERE ts > toDateTime('2026-03-04 06:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Injective deterministic wrapping: equality and negated equality both produce exact
-- atoms on concat(s, '_x') (the negated one cannot exclude granules at this size
-- because adjacent mark ranges overlap at the point, but the atom must stay exact).
CREATE TABLE test_floor_concat (s String) ENGINE = MergeTree
ORDER BY concat(s, '_x')
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_concat VALUES ('a'), ('b'), ('c'), ('d');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_concat WHERE s = 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_concat WHERE s = 'b' SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_concat WHERE s != 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_concat WHERE s != 'b' SETTINGS force_primary_key = 1;
SELECT count() FROM test_floor_concat WHERE s != 'b' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- IN and has over a direct key column.
CREATE TABLE test_floor_direct (x UInt32) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_direct SELECT number FROM numbers(16);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_direct WHERE x IN (3, 7, 11)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_direct WHERE x IN (3, 7, 11) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_direct WHERE has([3, 7, 11], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_direct WHERE has([3, 7, 11], x) SETTINGS force_primary_key = 1;

-- isNull / isNotNull over a Nullable key column.
CREATE TABLE test_floor_nullable (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree
ORDER BY ts
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_nullable SELECT toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL number DAY FROM numbers(8);
INSERT INTO test_floor_nullable VALUES (NULL), (NULL);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_nullable WHERE isNull(ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_nullable WHERE isNull(ts) SETTINGS force_primary_key = 1;
SELECT count() FROM test_floor_nullable WHERE isNull(ts) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_nullable WHERE isNotNull(ts)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_nullable WHERE isNotNull(ts) SETTINGS force_primary_key = 1;
SELECT count() FROM test_floor_nullable WHERE isNotNull(ts) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- empty / notEmpty over a String key.
CREATE TABLE test_floor_str (s String) ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_floor_str VALUES (''), ('a'), ('b'), ('c');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_str WHERE empty(s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_str WHERE empty(s) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_floor_str WHERE notEmpty(s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_floor_str WHERE notEmpty(s) SETTINGS force_primary_key = 1;
SELECT count() FROM test_floor_str WHERE notEmpty(s) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_floor_main;
DROP TABLE test_floor_wrap;
DROP TABLE test_floor_concat;
DROP TABLE test_floor_direct;
DROP TABLE test_floor_nullable;
DROP TABLE test_floor_str;
