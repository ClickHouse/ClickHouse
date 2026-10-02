-- Random settings limits: optimize_on_insert=(1, None); max_insert_threads=(None, 1)
-- An INSERT into ReplacingMergeTree, CollapsingMergeTree and VersionedCollapsingMergeTree with `optimize_on_insert`
-- keeps the rows of the block that a merge keeps, each with all of its columns, whether or not rows share a sorting key.

SET async_insert = 0;
SET log_queries = 1;

-- Unique keys in shuffled order: every row is kept, sorted.
CREATE TABLE t_unique (k UInt64, ver UInt32, s String, lc LowCardinality(String), a Array(LowCardinality(String)), f Nullable(Float64))
ENGINE = ReplacingMergeTree(ver) ORDER BY k;
SET log_comment = '05317_unique';
INSERT INTO t_unique VALUES (3, 1, 'c', 'lc3', ['x3'], 3.5), (1, 1, 'a', 'lc1', ['x1'], NULL), (4, 2, 'd', 'lc4', ['x4', 'y4'], 4.5), (2, 1, 'b', 'lc2', [], 2.5);
SELECT * FROM t_unique ORDER BY _part_offset;

-- Repeated keys: the row with the highest version (the last one among equal versions) is kept, with its own payload.
CREATE TABLE t_replace
(
    key UInt64, ver UInt8, n UInt64, s String, lc LowCardinality(String), m Map(String, UInt64),
    t Tuple(a UInt64, b String), j JSON, d Dynamic
)
ENGINE = ReplacingMergeTree(ver) ORDER BY key;
SET log_comment = '05317_replace';
INSERT INTO t_replace SELECT intHash32(number) % 1000, number % 7, number, toString(number), toString(number % 37), map('x', number),
    (number, toString(number)), concat('{"a":', toString(number), '}')::JSON, number::Dynamic
FROM numbers(5000);
SELECT
    count(),
    countIf((key, n) NOT IN (SELECT intHash32(number) % 1000, argMax(number, (number % 7, number)) FROM numbers(5000) GROUP BY 1)),
    countIf(s != toString(n) OR lc != toString(n % 37) OR m['x'] != n OR t.a != n OR t.b != toString(n) OR j.a::UInt64 != n OR d::UInt64 != n)
FROM t_replace;
SELECT arraySort(groupArray(key)) = groupArray(key) FROM (SELECT key FROM t_replace ORDER BY _part_offset);

-- Collapsing: a key keeps its last positive row, or its first negative row, or both, or nothing.
CREATE TABLE t_collapse (k UInt64, sign Int8, s String) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
SET log_comment = '05317_collapse';
INSERT INTO t_collapse VALUES (3, 1, 'f'), (1, 1, 'a'), (2, -1, 'd'), (3, 1, 'g'), (4, 1, 'k'), (1, -1, 'b'), (3, -1, 'h'), (2, 1, 'e'), (4, -1, 'l'), (3, -1, 'i'), (1, 1, 'c'), (3, 1, 'j');
SELECT * FROM t_collapse ORDER BY _part_offset;

-- A block whose rows all cancel out creates no part.
CREATE TABLE t_cancel (k UInt64, sign Int8, s String) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
SET log_comment = '05317_cancel';
INSERT INTO t_cancel VALUES (5, 1, 'x'), (5, -1, 'y');
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_cancel';

-- VersionedCollapsing: rows of the same key and version cancel in pairs.
CREATE TABLE t_versioned (k UInt64, ver UInt32, sign Int8, s String) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY k;
SET log_comment = '05317_versioned';
INSERT INTO t_versioned VALUES (3, 1, 1, 'f'), (1, 1, 1, 'a'), (2, 2, 1, 'd'), (1, 1, -1, 'b'), (2, 1, 1, 'c'), (3, 1, -1, 'e');
SELECT * FROM t_versioned ORDER BY _part_offset;

CREATE TABLE t_versioned_unique (k UInt64, ver UInt32, sign Int8, s String) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY k;
SET log_comment = '05317_versioned_unique';
INSERT INTO t_versioned_unique VALUES (11, 1, -1, 'y'), (10, 1, 1, 'x');
SELECT * FROM t_versioned_unique ORDER BY _part_offset;

-- Sorted runs of equal keys without a version: the last row of each run is kept.
CREATE TABLE t_runs (k UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY k;
SET log_comment = '05317_runs';
INSERT INTO t_runs SELECT intDiv(number, 10), toString(number) FROM numbers(5000);
SELECT count(), countIf(s != toString(k * 10 + 9)) FROM t_runs;

-- Without a sorting key all rows share it, so only the last one is kept.
CREATE TABLE t_no_key (k UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY tuple();
SET log_comment = '05317_no_key';
INSERT INTO t_no_key VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM t_no_key;

-- A table with key columns only.
CREATE TABLE t_key_only (k UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
SET log_comment = '05317_key_only';
INSERT INTO t_key_only SELECT number % 10 FROM numbers(100);
SELECT count(), sum(k) FROM t_key_only;

-- Invalid values are rejected even when no key repeats.
SET log_comment = '05317_invalid';
CREATE TABLE t_bad_sign (k UInt64, sign Int8, s String) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
INSERT INTO t_bad_sign VALUES (1, 1, 'a'), (2, 2, 'b'); -- { serverError INCORRECT_DATA }
CREATE TABLE t_bad_deleted (k UInt64, ver UInt32, is_deleted UInt8, s String) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k;
INSERT INTO t_bad_deleted VALUES (1, 1, 0, 'a'), (2, 1, 2, 'b'); -- { serverError INCORRECT_DATA }

-- Which INSERTs skipped the merge, and which merged the key columns only.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, max(ProfileEvents['MergeTreeDataWriterBlocksMergeSkipped']), max(ProfileEvents['MergeTreeDataWriterBlocksMergedOnKeyColumns'])
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment LIKE '05317\_%'
GROUP BY log_comment
ORDER BY log_comment;
