-- Tags: no-random-settings, no-random-merge-tree-settings

-- A Compact part is written in stripes: inside a stripe all granules of a column are adjacent.
-- The layout is checked by the order of the marks: every offset is replaced with its rank among all marks of the part.

DROP TABLE IF EXISTS t_compact_stripes;

CREATE TABLE t_compact_stripes (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4), s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'basic', compact_parts_max_granules_to_buffer = 2, merge_max_block_size = 4;

SYSTEM STOP MERGES t_compact_stripes;

-- One block of 7 rows: granules of 2, 2, 2, 1 rows in two stripes of 2 granules.
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('x', number) FROM numbers(7);

SELECT 'two granules per stripe';
WITH (
    SELECT arraySort(arrayDistinct(arrayConcat(groupArray(a.mark.1), groupArray(b.mark.1), groupArray(s.mark.1))))
    FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
    WHERE rows_in_granule > 0
) AS offsets
SELECT mark_number, rows_in_granule, indexOf(offsets, a.mark.1) AS rank_a, indexOf(offsets, b.mark.1) AS rank_b, indexOf(offsets, s.mark.1) AS rank_s
FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
WHERE rows_in_granule > 0
ORDER BY mark_number;

SELECT * FROM t_compact_stripes ORDER BY a;
SELECT b FROM t_compact_stripes WHERE a >= 3 ORDER BY b;
SELECT s FROM t_compact_stripes WHERE a = 5;
SELECT count() FROM t_compact_stripes WHERE a IN (1, 6);

-- A merge writes blocks of 4 rows (2 granules); a stripe of up to 3 granules is accumulated from two blocks.
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('y', number) FROM numbers(7, 5);
ALTER TABLE t_compact_stripes MODIFY SETTING compact_parts_max_granules_to_buffer = 3;
SYSTEM START MERGES t_compact_stripes;
OPTIMIZE TABLE t_compact_stripes FINAL;

SELECT 'merged, three granules per stripe';
WITH (
    SELECT arraySort(arrayDistinct(arrayConcat(groupArray(a.mark.1), groupArray(b.mark.1), groupArray(s.mark.1))))
    FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
    WHERE rows_in_granule > 0
) AS offsets
SELECT mark_number, rows_in_granule, indexOf(offsets, a.mark.1) AS rank_a, indexOf(offsets, b.mark.1) AS rank_b, indexOf(offsets, s.mark.1) AS rank_s
FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
WHERE rows_in_granule > 0
ORDER BY mark_number;

SELECT count(), sum(a), sum(b), sum(length(s)) FROM t_compact_stripes;
SELECT s FROM t_compact_stripes WHERE a IN (3, 9) ORDER BY a;

DROP TABLE t_compact_stripes;

-- One granule per stripe: all columns of a granule are adjacent.
CREATE TABLE t_compact_stripes (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4), s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'basic', compact_parts_max_granules_to_buffer = 1;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('x', number) FROM numbers(7);

SELECT 'one granule per stripe';
WITH (
    SELECT arraySort(arrayDistinct(arrayConcat(groupArray(a.mark.1), groupArray(b.mark.1), groupArray(s.mark.1))))
    FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
    WHERE rows_in_granule > 0
) AS offsets
SELECT mark_number, rows_in_granule, indexOf(offsets, a.mark.1) AS rank_a, indexOf(offsets, b.mark.1) AS rank_b, indexOf(offsets, s.mark.1) AS rank_s
FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
WHERE rows_in_granule > 0
ORDER BY mark_number;

DROP TABLE t_compact_stripes;

-- The byte limit also ends a stripe: with the limit of 1 byte every granule of a big block becomes a stripe.
CREATE TABLE t_compact_stripes (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4), s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'basic', compact_parts_max_granules_to_buffer = 100, compact_parts_max_bytes_to_buffer = 1;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('x', number) FROM numbers(7);

SELECT 'byte limit';
WITH (
    SELECT arraySort(arrayDistinct(arrayConcat(groupArray(a.mark.1), groupArray(b.mark.1), groupArray(s.mark.1))))
    FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
    WHERE rows_in_granule > 0
) AS offsets
SELECT mark_number, rows_in_granule, indexOf(offsets, a.mark.1) AS rank_a, indexOf(offsets, b.mark.1) AS rank_b, indexOf(offsets, s.mark.1) AS rank_s
FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
WHERE rows_in_granule > 0
ORDER BY mark_number;

DROP TABLE t_compact_stripes;

-- Only a whole number of granules is written as a stripe, so the beginning of an incomplete granule stays in the
-- buffer until the next blocks complete it. Here the byte limit of a stripe is reached while the buffer holds
-- complete granules and the beginning of the next one.
CREATE TABLE t_compact_stripes (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 7, min_bytes_for_wide_part = '1G',
    ratio_of_defaults_for_sparse_serialization = 1.0, serialization_info_version = 'basic', merge_max_block_size = 3,
    compact_parts_max_granules_to_buffer = 100, compact_parts_max_bytes_to_buffer = 150;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT number, number * 10 FROM numbers(12);
INSERT INTO t_compact_stripes SELECT number, number * 10 FROM numbers(12, 12);
SYSTEM START MERGES t_compact_stripes;
OPTIMIZE TABLE t_compact_stripes FINAL;

SELECT 'incomplete granule stays in the buffer';
SELECT mark_number, rows_in_granule FROM mergeTreeIndex(currentDatabase(), t_compact_stripes, with_marks = true)
WHERE rows_in_granule > 0 ORDER BY mark_number;
SELECT count(), sum(a), sum(b) FROM t_compact_stripes;

DROP TABLE t_compact_stripes;

-- A part with enough granules (at least `merge_tree_compact_parts_min_granules_to_multibuffer_read`, or at least as many as
-- the columns to read) is read with a buffer per column. The number of created read buffers (one per column, plus one for
-- the marks) shows which reader is used.
CREATE TABLE t_compact_stripes (a UInt64, b UInt64, s String, json JSON)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('x', number % 7), concat('{"k":', toString(number), ',"n":{"m":"', toString(number % 3), '"}}')::JSON FROM numbers(40);

SELECT 'multiple buffers';
SYSTEM CLEAR MARK CACHE;
SELECT sum(a), sum(b), sum(length(s)), sum(json.k::UInt64), countDistinct(json.n.m), max(toString(json)) FROM t_compact_stripes
SETTINGS max_threads = 1, log_comment = '05113 multiple buffers, four columns';

SYSTEM CLEAR MARK CACHE;
SELECT sum(a), sum(length(s)) FROM t_compact_stripes
SETTINGS max_threads = 1, log_comment = '05113 multiple buffers, two columns';

DROP TABLE t_compact_stripes;

-- A part with fewer granules than columns to read is read with a single buffer.
CREATE TABLE t_compact_stripes (a UInt64, b UInt64, s String, json JSON)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT number, number * 10, repeat('x', number % 7), concat('{"k":', toString(number), ',"n":{"m":"', toString(number % 3), '"}}')::JSON FROM numbers(4);

SELECT 'single buffer';
SYSTEM CLEAR MARK CACHE;
SELECT sum(a), sum(b), sum(length(s)), sum(json.k::UInt64) FROM t_compact_stripes
SETTINGS max_threads = 1, log_comment = '05113 single buffer, four columns';

DROP TABLE t_compact_stripes;

-- The first column of a part may have several substreams (the marks of all its substreams of a granule are adjacent
-- in both layouts), so the stripes are detected by the marks of the first substreams of the first two columns.
CREATE TABLE t_compact_stripes (arr Array(UInt64), a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP MERGES t_compact_stripes;
INSERT INTO t_compact_stripes SELECT range(number % 4), number, repeat('x', number % 7) FROM numbers(40);

SELECT 'multi-substream first column';
SYSTEM CLEAR MARK CACHE;
SELECT sum(length(arr)), sum(a), sum(length(s)) FROM t_compact_stripes
SETTINGS max_threads = 1, log_comment = '05113 multiple buffers, multi-substream first column';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['CreatedReadBufferOrdinary'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05113 %'
ORDER BY event_time_microseconds;

DROP TABLE t_compact_stripes;
