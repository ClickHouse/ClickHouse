-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 16;

-- { echo }

-- Reverse (descending) sorting key with several overlapping parts: every part holds the same key set,
-- so each key appears in every in-order stream and the streams must be merged descending before LIMIT BY.
DROP TABLE IF EXISTS test_reverse_single;
CREATE TABLE test_reverse_single (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_reverse_single;
INSERT INTO test_reverse_single SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_reverse_single SELECT number % 100, number+1000 FROM numbers(1000);
INSERT INTO test_reverse_single SELECT number % 100, number+2000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT k FROM test_reverse_single LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(k) FROM test_reverse_single) FROM (SELECT k FROM test_reverse_single LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 1);
SELECT (SELECT groupArray(k) FROM (SELECT k FROM (SELECT k FROM test_reverse_single LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 0) ORDER BY k)) = (SELECT groupArray(k) FROM (SELECT k FROM (SELECT k FROM test_reverse_single LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 1) ORDER BY k));
DROP TABLE test_reverse_single;

-- OFFSET across reverse-key streams: the per-stream pre-filter keeps `length + offset` rows per key. The
-- optimized output must equal the two smallest `v` per key after dropping the first, computed with a window function.
DROP TABLE IF EXISTS test_reverse_offset;
CREATE TABLE test_reverse_offset (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_reverse_offset;
INSERT INTO test_reverse_offset SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_reverse_offset SELECT number % 100, number+1000 FROM numbers(1000);
INSERT INTO test_reverse_offset SELECT number % 100, number+2000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT k FROM test_reverse_offset LIMIT 2 OFFSET 1 BY k SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT (SELECT groupArray((k, v)) FROM (SELECT k, v FROM test_reverse_offset ORDER BY k DESC, v LIMIT 2 OFFSET 1 BY k SETTINGS optimize_limit_by_in_order = 1)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM (SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM test_reverse_offset) WHERE rn > 1 AND rn <= 3 ORDER BY k DESC, v));
DROP TABLE test_reverse_offset;

-- Multi-column reverse key (both columns descending), LIMIT BY the length-1 prefix.
DROP TABLE IF EXISTS test_reverse_multi_col;
CREATE TABLE test_reverse_multi_col (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC, b DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_reverse_multi_col;
INSERT INTO test_reverse_multi_col SELECT number % 50, number % 7, number      FROM numbers(1000);
INSERT INTO test_reverse_multi_col SELECT number % 50, number % 7, number+1000 FROM numbers(1000);
INSERT INTO test_reverse_multi_col SELECT number % 50, number % 7, number+2000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a FROM test_reverse_multi_col LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_reverse_multi_col) FROM (SELECT a FROM test_reverse_multi_col LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1);
DROP TABLE test_reverse_multi_col;

-- Mixed-direction key: ascending leading column, descending second column. LIMIT BY both.
DROP TABLE IF EXISTS test_reverse_mixed;
CREATE TABLE test_reverse_mixed (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a, b DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_reverse_mixed;
INSERT INTO test_reverse_mixed SELECT number % 50, number % 7, number      FROM numbers(1000);
INSERT INTO test_reverse_mixed SELECT number % 50, number % 7, number+1000 FROM numbers(1000);
INSERT INTO test_reverse_mixed SELECT number % 50, number % 7, number+2000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b FROM test_reverse_mixed LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact((a, b)) FROM test_reverse_mixed) FROM (SELECT a, b FROM test_reverse_mixed LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1);
SELECT (SELECT groupArray((a, b)) FROM (SELECT a, b FROM (SELECT a, b FROM test_reverse_mixed LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 0) ORDER BY a, b)) = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM (SELECT a, b FROM test_reverse_mixed LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1) ORDER BY a, b));
DROP TABLE test_reverse_mixed;

-- Merge engine over children whose sort keys disagree on direction (one ascending, one reverse).
DROP TABLE IF EXISTS test_merge_mixed_dir_part_1;
DROP TABLE IF EXISTS test_merge_mixed_dir_part_2;
DROP TABLE IF EXISTS test_merge_mixed_dir_wrap;
CREATE TABLE test_merge_mixed_dir_part_1 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE test_merge_mixed_dir_part_2 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC) SETTINGS allow_experimental_reverse_key = 1;
INSERT INTO test_merge_mixed_dir_part_1 SELECT number % 20, number      FROM numbers(1000);
INSERT INTO test_merge_mixed_dir_part_2 SELECT number % 20, number+1000 FROM numbers(1000);
CREATE TABLE test_merge_mixed_dir_wrap AS test_merge_mixed_dir_part_1 ENGINE = Merge(currentDatabase(), '^test_merge_mixed_dir_part_[12]$');
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a FROM test_merge_mixed_dir_wrap LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_merge_mixed_dir_wrap) FROM (SELECT a FROM test_merge_mixed_dir_wrap LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1);
DROP TABLE test_merge_mixed_dir_wrap;
DROP TABLE test_merge_mixed_dir_part_1;
DROP TABLE test_merge_mixed_dir_part_2;
