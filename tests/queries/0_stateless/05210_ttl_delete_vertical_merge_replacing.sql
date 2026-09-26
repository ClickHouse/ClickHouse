-- A rows-TTL merge on `ReplacingMergeTree` may use the vertical algorithm, and must return the
-- rows a horizontal merge returns. Each case keeps all versions of a key in one part, so a single
-- merge picks the winner and applies the TTL.
--
-- Test 1 carries a control: a clone of its table with `vertical_merge_optimize_ttl_delete` off,
-- filled from the case table so neither the schema nor the rows can drift apart, and asserted to
-- hold the same rows afterwards. `test1_control_algo` is `Horizontal` - that is what makes it a
-- control rather than a second copy of the case.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: an expired winning version drops the key; the older live version is not resurrected.
DROP TABLE IF EXISTS t_ttl_vert_repl_win;
DROP TABLE IF EXISTS t_ttl_vert_repl_off;

CREATE TABLE t_ttl_vert_repl_win
(
    id UInt64,
    ver UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE t_ttl_vert_repl_off AS t_ttl_vert_repl_win;
ALTER TABLE t_ttl_vert_repl_off MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

-- Keys 0..99 hold a live version 1 and an expired version 2, so the winner is expired and the key
-- must disappear. Keys 100..199 hold an expired version 1 and a live version 2, so they survive.
-- A background TTL merge would delete rows before the control copies them.
SYSTEM STOP TTL MERGES t_ttl_vert_repl_win;

INSERT INTO t_ttl_vert_repl_win
SELECT
    number % 200 AS id,
    intDiv(number, 200) + 1 AS ver,
    if((id < 100) = (ver = 1), now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00') AS d,
    ver, ver, ver
FROM numbers(400);

INSERT INTO t_ttl_vert_repl_off SELECT * FROM t_ttl_vert_repl_win;
SYSTEM START TTL MERGES t_ttl_vert_repl_win;

OPTIMIZE TABLE t_ttl_vert_repl_win FINAL;
OPTIMIZE TABLE t_ttl_vert_repl_off FINAL;

SELECT 'test1_count', count() FROM t_ttl_vert_repl_win;
SELECT 'test1_id_range', min(id), max(id) FROM t_ttl_vert_repl_win;
SELECT 'test1_versions', groupUniqArray(ver) FROM t_ttl_vert_repl_win;
SELECT 'test1_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_win;
SELECT 'test1_expired_left', count() FROM t_ttl_vert_repl_win WHERE d + INTERVAL 1 DAY <= now();
SELECT 'test1_parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_win' AND active;
SELECT 'test1_control_same_rows', arraySort(groupArray((id, ver, d, c1, c2, c3)))
    = (SELECT arraySort(groupArray((id, ver, d, c1, c2, c3))) FROM t_ttl_vert_repl_off)
FROM t_ttl_vert_repl_win;

SYSTEM FLUSH LOGS part_log;
-- Exclude `TTLDropMerge`: a part whose rows are all expired is dropped by a short-circuit that
-- always uses the `Horizontal` algorithm.
SELECT 'test1_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_win' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;
SELECT 'test1_control_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_repl_win;
DROP TABLE t_ttl_vert_repl_off;

-- Test 2: no version column - every comparison ties, so the winner comes from the physical-order
-- rule, which test 1 never reaches because its versions always differ.
DROP TABLE IF EXISTS t_ttl_vert_repl_no_version;

CREATE TABLE t_ttl_vert_repl_no_version
(
    id UInt64,
    seq UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Rows arrive ordered by (id, seq), so seq = 2 is the winner of every key. Keys 0..99 have an
-- expired winner and must be dropped; keys 100..199 have a live winner and survive with seq = 2.
INSERT INTO t_ttl_vert_repl_no_version
SELECT
    number % 200 AS id,
    intDiv(number, 200) + 1 AS seq,
    if((id < 100) = (seq = 1), now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00') AS d,
    seq, seq, seq
FROM numbers(400)
ORDER BY id, seq;

OPTIMIZE TABLE t_ttl_vert_repl_no_version FINAL;

SELECT 'test2_count', count() FROM t_ttl_vert_repl_no_version;
SELECT 'test2_id_range', min(id), max(id) FROM t_ttl_vert_repl_no_version;
SELECT 'test2_seqs', groupUniqArray(seq) FROM t_ttl_vert_repl_no_version;
SELECT 'test2_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_no_version;

DROP TABLE t_ttl_vert_repl_no_version;

-- Test 3: a duplicate-free part that sorts entirely before the others takes the chunk pass-through
-- path, which applies the TTL predicate per row instead of keeping the whole chunk.
DROP TABLE IF EXISTS t_ttl_vert_repl_disjoint;

CREATE TABLE t_ttl_vert_repl_disjoint
(
    id UInt64,
    ver UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- The pass-through needs a source part of non-zero level, so build one while TTL merges are off:
-- with the TTL blocker set the merge keeps every row and only raises the level.
SYSTEM STOP TTL MERGES t_ttl_vert_repl_disjoint;
INSERT INTO t_ttl_vert_repl_disjoint
SELECT number, 1, if(number % 2, '2000-01-01 00:00:00', now() + INTERVAL 1 YEAR), 1, 1, 1 FROM numbers(100);
OPTIMIZE TABLE t_ttl_vert_repl_disjoint FINAL;
SELECT 'test3_level1_rows', sum(rows), max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_disjoint' AND active;

-- A second, strictly greater key range keeps the level-1 part totally less than the rest.
INSERT INTO t_ttl_vert_repl_disjoint
SELECT number + 200, 1, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), 1, 1, 1 FROM numbers(100);

SYSTEM START TTL MERGES t_ttl_vert_repl_disjoint;
OPTIMIZE TABLE t_ttl_vert_repl_disjoint FINAL;

SELECT 'test3_count', count() FROM t_ttl_vert_repl_disjoint;
SELECT 'test3_expired_left', count() FROM t_ttl_vert_repl_disjoint WHERE d + INTERVAL 1 DAY <= now();
SELECT 'test3_ids', sum(id), sum(c1 + c2 + c3) FROM t_ttl_vert_repl_disjoint;
SELECT 'test3_odd_even', countIf(id < 200 AND id % 2 = 0), countIf(id >= 200 AND id % 2 = 1) FROM t_ttl_vert_repl_disjoint;

SYSTEM FLUSH LOGS part_log;
-- Both merges must be vertical; the second one is the one reaching the pass-through.
SELECT 'test3_algos', countDistinct(merge_algorithm), any(merge_algorithm) FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_disjoint' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge';

DROP TABLE t_ttl_vert_repl_disjoint;

-- Test 4: rows TTL with a WHERE clause - the winner is expired for every key, but only the rows
-- the WHERE clause selects may be deleted.
DROP TABLE IF EXISTS t_ttl_vert_repl_where;

CREATE TABLE t_ttl_vert_repl_where
(
    id UInt64,
    ver UInt64,
    d DateTime,
    keep UInt8,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
TTL d + INTERVAL 1 DAY DELETE WHERE keep = 0
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_ttl_vert_repl_where
SELECT number % 100 AS id, intDiv(number, 100) + 1 AS ver, '2000-01-01 00:00:00', id % 2, ver, ver, ver
FROM numbers(200);

OPTIMIZE TABLE t_ttl_vert_repl_where FINAL;

SELECT 'test4_count', count() FROM t_ttl_vert_repl_where;
SELECT 'test4_keep', groupUniqArray(keep), groupUniqArray(ver) FROM t_ttl_vert_repl_where;
SELECT 'test4_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_where;

SYSTEM FLUSH LOGS part_log;
SELECT 'test4_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_where' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_repl_where;
