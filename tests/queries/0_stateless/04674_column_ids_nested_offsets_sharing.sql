-- Tags: no-random-merge-tree-settings
-- Flattened Nested siblings ("n.x", "n.y") share one on-disk offsets stream. Under column ids they
-- are separate top-level columns with separate substreams caches, so the reader has to hand the
-- offsets it read to the siblings still to come. Enough rows for several marks and blocks: with a
-- single mark the siblings never disagree and the bug hides.

SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_nested_ids;
DROP TABLE IF EXISTS t_nested_plain;

CREATE TABLE t_nested_ids (k UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192;

CREATE TABLE t_nested_plain (k UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192;

-- Two parts of differing array lengths, so a wrong offsets read shifts the elements visibly.
INSERT INTO t_nested_ids SELECT number, [number, number + 1, number + 2], [toString(number), 'b', 'c'] FROM numbers(100000);
INSERT INTO t_nested_ids SELECT number, [number, number + 1], [toString(number), 'z'] FROM numbers(100000, 100000);
INSERT INTO t_nested_plain SELECT number, [number, number + 1, number + 2], [toString(number), 'b', 'c'] FROM numbers(100000);
INSERT INTO t_nested_plain SELECT number, [number, number + 1], [toString(number), 'z'] FROM numbers(100000, 100000);

-- Marked read of both siblings at once: the sibling that reads the shared stream second must agree
-- with the first on the row count, or the range reader's consistency check fires.
SELECT 'select matches plain',
       (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_ids) = (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_plain);

-- Markless whole-part read: the merge reads every column of the part in one range.
OPTIMIZE TABLE t_nested_ids FINAL;
OPTIMIZE TABLE t_nested_plain FINAL;

SELECT 'one part each', (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_ids' AND active),
                        (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_plain' AND active);

SELECT 'merge matches plain',
       (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_ids) = (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_plain);

-- Spot-check the array boundaries the offsets stream defines, either side of the part split.
SELECT k, n.x, n.y FROM t_nested_ids WHERE k IN (0, 99999, 100000, 199999) ORDER BY k;

-- The other markless whole-part reader: a mutation rewrites the part, reading every column through
-- the same shared offsets stream, granule by granule -- so a sibling left on a stale stream position
-- shows up here and not in the merge above.
ALTER TABLE t_nested_ids DELETE WHERE k = 99999 SETTINGS mutations_sync = 2;
ALTER TABLE t_nested_plain DELETE WHERE k = 99999 SETTINGS mutations_sync = 2;
SELECT 'mutation matches plain',
       (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_ids) = (SELECT sum(cityHash64(k, n.x, n.y)) FROM t_nested_plain);
SELECT k, n.x, n.y FROM t_nested_ids WHERE k IN (0, 199999) ORDER BY k;

DROP TABLE t_nested_ids;
DROP TABLE t_nested_plain;
