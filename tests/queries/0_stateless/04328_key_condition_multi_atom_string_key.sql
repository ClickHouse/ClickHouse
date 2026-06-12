-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_str_sub;
DROP TABLE IF EXISTS test_str_hash;

CREATE TABLE test_str_sub (s String) ENGINE = MergeTree
ORDER BY (substring(s, 1, 2), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_str_sub VALUES (''), ('aaa'), ('aab'), ('ab_'), ('aba'), ('abc'), ('abcz'), ('abz'), ('aca'), ('acd'), ('baa'), ('xyz');

-- Equality: the direct key column atom on s is exact (the deterministic substring
-- candidate is not added because a direct candidate already exists).
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s = 'abc') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE s = 'abc' SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s IN ('abc', 'xyz')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE s IN ('abc', 'xyz') SETTINGS force_primary_key = 1;

-- Pattern functions: perfect prefix.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s LIKE 'ab%') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE s LIKE 'ab%' SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE startsWith(s, 'ab')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE startsWith(s, 'ab') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE match(s, '^ab')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE match(s, '^ab') SETTINGS force_primary_key = 1;

-- Common prefix of alternation branches.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE match(s, '^(abc|abd)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE match(s, '^(abc|abd)') SETTINGS force_primary_key = 1;

-- notLike requires a perfect prefix and produces an exact complement range.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s NOT LIKE 'ab%') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%' SETTINGS enable_analyzer = 1;
SELECT count() FROM test_str_sub WHERE s NOT LIKE 'ab%' SETTINGS force_primary_key = 1;
SELECT count() FROM test_str_sub WHERE s NOT LIKE 'ab%' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Imperfect prefix: the atom is relaxed; results must stay exact.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s LIKE 'ab%z') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE s LIKE 'ab%z';
SELECT count() FROM test_str_sub WHERE s LIKE 'ab%z' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- No extractable prefix: full scan.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE s LIKE '%ab') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE s LIKE '%ab';
SELECT count() FROM test_str_sub WHERE s LIKE '%ab' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- has(const_array, key) builds wrapped set atoms for the substring key column too.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE has(['abc', 'xyz'], s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE has(['abc', 'xyz'], s) SETTINGS force_primary_key = 1;

-- Constant on the left: inequality is mirrored.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE 'ab' < s) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE 'ab' < s SETTINGS force_primary_key = 1;

-- empty/notEmpty over the String key column.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE empty(s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE empty(s) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_sub WHERE notEmpty(s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_sub WHERE notEmpty(s) SETTINGS force_primary_key = 1;
SELECT count() FROM test_str_sub WHERE notEmpty(s) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Key is only a non-injective hash of the column: equality prunes through the hash,
-- negated equality is relaxed and must not prune away rows.
CREATE TABLE test_str_hash (s String) ENGINE = MergeTree
ORDER BY cityHash64(s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_str_hash VALUES ('a'), ('b'), ('c'), ('d');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_hash WHERE s = 'a') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_hash WHERE s = 'a' SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_hash WHERE s != 'a') WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_hash WHERE s != 'a';
SELECT count() FROM test_str_hash WHERE s != 'a' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Mixed key: the direct column plus a non-injective hash of it. IN and has build the
-- direct set atom on s plus the wrapped set atom on cityHash64(s).
CREATE TABLE test_str_mixed (s String) ENGINE = MergeTree
ORDER BY (cityHash64(s), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_str_mixed VALUES ('a'), ('b'), ('c'), ('d'), ('e'), ('f');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_mixed WHERE s IN ('b', 'e')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_mixed WHERE s IN ('b', 'e') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_str_mixed WHERE has(['b', 'e'], s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_str_mixed WHERE has(['b', 'e'], s) SETTINGS force_primary_key = 1;

DROP TABLE test_str_sub;
DROP TABLE test_str_hash;
DROP TABLE test_str_mixed;
