-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_tn_uint8;
DROP TABLE IF EXISTS test_tn_compose;
DROP TABLE IF EXISTS test_tn_int8;
DROP TABLE IF EXISTS test_tn_nullable;
DROP TABLE IF EXISTS test_tn_fixed;
DROP TABLE IF EXISTS test_tn_parse;
DROP TABLE IF EXISTS test_tn_dt64;
DROP TABLE IF EXISTS test_tn_date32;
DROP TABLE IF EXISTS test_tn_overflow;

-- Float literals against an integer key.
CREATE TABLE test_tn_uint8 (x UInt8) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_uint8 SELECT number FROM numbers(256);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x = 5.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x = 5.5;
SELECT count() FROM test_tn_uint8 WHERE x = 5.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x != 5.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x != 5.5;
SELECT count() FROM test_tn_uint8 WHERE x != 5.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x < 5.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x < 5.5 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x > 5.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x > 5.5 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x = 5.0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x = 5.0 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x < -0.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x < -0.5;
SELECT count() FROM test_tn_uint8 WHERE x < -0.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x >= -0.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x >= -0.5;
SELECT count() FROM test_tn_uint8 WHERE x >= -0.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x < 256.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x < 256.5;
SELECT count() FROM test_tn_uint8 WHERE x < 256.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_uint8 WHERE x > 256.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_uint8 WHERE x > 256.5;
SELECT count() FROM test_tn_uint8 WHERE x > 256.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- An ALWAYS fold composes with prunable atoms from the other predicate.
CREATE TABLE test_tn_compose (ts DateTime('UTC'), x UInt8) ENGINE = MergeTree
ORDER BY (toDate(ts), x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_compose SELECT toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR, number FROM numbers(12);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_compose WHERE x > 300.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_compose WHERE x > 300.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC');
SELECT count() FROM test_tn_compose WHERE x > 300.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_compose WHERE x > -0.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_compose WHERE x > -0.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC');
SELECT count() FROM test_tn_compose WHERE x > -0.5 AND ts > toDateTime('2026-03-02 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Supertype conversion appends an internal cast to the monotonic chain.
CREATE TABLE test_tn_int8 (k Int8) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_int8 SELECT number - 5 FROM numbers(11);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_int8 WHERE k < 1000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_int8 WHERE k < 1000 SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_int8 WHERE k < 1000 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_int8 WHERE k = 1000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_int8 WHERE k = 1000 SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_int8 WHERE k = 1000 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_int8 WHERE k > -1000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_int8 WHERE k > -1000 SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_int8 WHERE k > -1000 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- The same with a Nullable key.
CREATE TABLE test_tn_nullable (k Nullable(Int8)) ENGINE = MergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_nullable SELECT number - 5 FROM numbers(11);
INSERT INTO test_tn_nullable VALUES (NULL);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_nullable WHERE k < 1000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_nullable WHERE k < 1000;
SELECT count() FROM test_tn_nullable WHERE k < 1000 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- FixedString key with prefix patterns: the constant must not be padded.
CREATE TABLE test_tn_fixed (s FixedString(3)) ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_fixed VALUES ('aaa'), ('abc'), ('abd'), ('xyz');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_fixed WHERE s LIKE 'ab%') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_fixed WHERE s LIKE 'ab%' SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_fixed WHERE s LIKE 'ab%' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_fixed WHERE startsWith(s, 'ab')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_fixed WHERE startsWith(s, 'ab') SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_fixed WHERE startsWith(s, 'ab') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- String column parsed into a Date key.
CREATE TABLE test_tn_parse (s String) ENGINE = MergeTree
ORDER BY toDate(s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_parse SELECT toString(toDate('2026-01-01') + intDiv(number, 2)) FROM numbers(10);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s = '2026-01-03') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s = '2026-01-03' SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_parse WHERE s = '2026-01-03' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03' SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Unparsable constants must not produce atoms or exceptions.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s = 'garbage') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s = 'garbage';
SELECT count() FROM test_tn_parse WHERE s = 'garbage' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s > 'garbage') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s > 'garbage';
SELECT count() FROM test_tn_parse WHERE s > 'garbage' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- With session_timezone set, datetime-parsing chains over String are skipped per candidate;
-- results must be unchanged.
SET session_timezone = 'UTC';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s = '2026-01-03') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s = '2026-01-03';
SELECT count() FROM test_tn_parse WHERE s = '2026-01-03' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03';
SELECT count() FROM test_tn_parse WHERE s >= '2026-01-03' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SET session_timezone = DEFAULT;

-- DateTime64 key vs DateTime constant and the reverse: compared accurately without cast.
CREATE TABLE test_tn_dt64 (ts64 DateTime64(3, 'UTC'), ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (ts64, ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_dt64 SELECT toDateTime64('2026-03-01 00:00:00', 3, 'UTC') + INTERVAL number HOUR, toDateTime('2026-03-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(8);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_dt64 WHERE ts64 = toDateTime('2026-03-01 03:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_dt64 WHERE ts64 = toDateTime('2026-03-01 03:00:00', 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_dt64 WHERE ts >= toDateTime64('2026-03-01 03:00:00', 3, 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_dt64 WHERE ts >= toDateTime64('2026-03-01 03:00:00', 3, 'UTC') SETTINGS force_primary_key = 1;

-- Date32 key with a pre-epoch constant.
CREATE TABLE test_tn_date32 (d Date32) ENGINE = MergeTree
ORDER BY d
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_date32 VALUES ('1900-01-01'), ('1950-06-15'), ('2000-01-01');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_date32 WHERE d < toDate32('1950-01-01')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_date32 WHERE d < toDate32('1950-01-01') SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_date32 WHERE d < toDate32('1950-01-01') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- toDate over DateTime64 overflows the Date range for far-future constants:
-- the chain must be rejected instead of producing a wrapping value.
CREATE TABLE test_tn_overflow (ts64 DateTime64(3, 'UTC')) ENGINE = MergeTree
ORDER BY toDate(ts64)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_overflow SELECT toDateTime64('2026-03-01 00:00:00', 3, 'UTC') + INTERVAL number DAY FROM numbers(4);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_overflow WHERE ts64 > toDateTime64('2200-01-01 00:00:00', 3, 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_overflow WHERE ts64 > toDateTime64('2200-01-01 00:00:00', 3, 'UTC');
SELECT count() FROM test_tn_overflow WHERE ts64 > toDateTime64('2200-01-01 00:00:00', 3, 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Type normalization through multi-atom candidates: each candidate normalizes its
-- constant independently.

-- Integer key with a monotonic conversion sibling. An integral float produces both
-- atoms with per-candidate bounds: the direct atom is rewritten exactly (> 5.0
-- becomes >= 6), the transformed candidate is weakened (toUInt64(x) >= 5).
DROP TABLE IF EXISTS test_tn_multi_int;
CREATE TABLE test_tn_multi_int (x UInt8) ENGINE = MergeTree
ORDER BY (toUInt64(x), x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_multi_int SELECT number FROM numbers(20);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_multi_int WHERE x > 5.0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_multi_int WHERE x > 5.0 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_multi_int WHERE x = 7.0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_multi_int WHERE x = 7.0 SETTINGS force_primary_key = 1;

-- A fractional float cannot be cast accurately into the toUInt64 chain, so that
-- candidate is rejected and only the rewritten direct atom remains.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_multi_int WHERE x > 5.5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_multi_int WHERE x > 5.5 SETTINGS force_primary_key = 1;
SELECT count() FROM test_tn_multi_int WHERE x > 5.5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Date constant against a DateTime multi-expression key: the supertype conversion on
-- the direct atom and the transformed constants of the other candidates coexist.
DROP TABLE IF EXISTS test_tn_multi_date;
CREATE TABLE test_tn_multi_date (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_multi_date SELECT toDateTime('2026-01-08 00:00:00', 'UTC') + INTERVAL number * 6 HOUR FROM numbers(16);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_multi_date WHERE ts >= toDate('2026-01-10')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_multi_date WHERE ts >= toDate('2026-01-10') SETTINGS force_primary_key = 1;

-- String-parsing chain candidate plus the direct String atom.
DROP TABLE IF EXISTS test_tn_multi_parse;
CREATE TABLE test_tn_multi_parse (s String) ENGINE = MergeTree
ORDER BY (toDate(s), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tn_multi_parse SELECT toString(toDate('2026-01-01') + intDiv(number, 2)) FROM numbers(10);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tn_multi_parse WHERE s >= '2026-01-03') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_tn_multi_parse WHERE s >= '2026-01-03' SETTINGS force_primary_key = 1;

DROP TABLE test_tn_uint8;
DROP TABLE test_tn_compose;
DROP TABLE test_tn_int8;
DROP TABLE test_tn_nullable;
DROP TABLE test_tn_fixed;
DROP TABLE test_tn_parse;
DROP TABLE test_tn_dt64;
DROP TABLE test_tn_date32;
DROP TABLE test_tn_overflow;
DROP TABLE test_tn_multi_int;
DROP TABLE test_tn_multi_date;
DROP TABLE test_tn_multi_parse;
