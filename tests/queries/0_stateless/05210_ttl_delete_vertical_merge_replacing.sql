-- A rows-TTL merge on `ReplacingMergeTree` may use the vertical algorithm, and must return the
-- rows a horizontal merge returns. Each case keeps all versions of a key in one part, so a single
-- merge picks the winner and applies the TTL.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: an expired winning version drops the key; the older live version is not resurrected.
DROP TABLE IF EXISTS t_ttl_vert_repl_win;

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

-- Keys 0..99 hold a live version 1 and an expired version 2, so the winner is expired and the key
-- must disappear. Keys 100..199 hold an expired version 1 and a live version 2, so they survive.
INSERT INTO t_ttl_vert_repl_win
SELECT
    number % 200 AS id,
    intDiv(number, 200) + 1 AS ver,
    if((id < 100) = (ver = 1), now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00') AS d,
    ver, ver, ver
FROM numbers(400);

OPTIMIZE TABLE t_ttl_vert_repl_win FINAL;

SELECT 'test1_count', count() FROM t_ttl_vert_repl_win;
SELECT 'test1_id_range', min(id), max(id) FROM t_ttl_vert_repl_win;
SELECT 'test1_versions', groupUniqArray(ver) FROM t_ttl_vert_repl_win;
SELECT 'test1_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_win;
SELECT 'test1_expired_left', count() FROM t_ttl_vert_repl_win WHERE d + INTERVAL 1 DAY <= now();
SELECT 'test1_parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_win' AND active;

SYSTEM FLUSH LOGS part_log;
-- Exclude `TTLDropMerge`: a part whose rows are all expired is dropped by a short-circuit that
-- always uses the `Horizontal` algorithm.
SELECT 'test1_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_win' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_repl_win;

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

-- Test 4: a duplicate-free part that sorts entirely before the others takes the chunk pass-through
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
SELECT 'test4_level1_rows', sum(rows), max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_disjoint' AND active;

-- A second, strictly greater key range keeps the level-1 part totally less than the rest.
INSERT INTO t_ttl_vert_repl_disjoint
SELECT number + 200, 1, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), 1, 1, 1 FROM numbers(100);

SYSTEM START TTL MERGES t_ttl_vert_repl_disjoint;
OPTIMIZE TABLE t_ttl_vert_repl_disjoint FINAL;

SELECT 'test4_count', count() FROM t_ttl_vert_repl_disjoint;
SELECT 'test4_expired_left', count() FROM t_ttl_vert_repl_disjoint WHERE d + INTERVAL 1 DAY <= now();
SELECT 'test4_ids', sum(id), sum(c1 + c2 + c3) FROM t_ttl_vert_repl_disjoint;
SELECT 'test4_odd_even', countIf(id < 200 AND id % 2 = 0), countIf(id >= 200 AND id % 2 = 1) FROM t_ttl_vert_repl_disjoint;

SYSTEM FLUSH LOGS part_log;
-- Both merges must be vertical; the second one is the one reaching the pass-through.
SELECT 'test4_algos', countDistinct(merge_algorithm), any(merge_algorithm) FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_disjoint' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge';

DROP TABLE t_ttl_vert_repl_disjoint;

-- Test 5: rows TTL with a WHERE clause - the winner is expired for every key, but only the rows
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

SELECT 'test5_count', count() FROM t_ttl_vert_repl_where;
SELECT 'test5_keep', groupUniqArray(keep), groupUniqArray(ver) FROM t_ttl_vert_repl_where;
SELECT 'test5_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_where;

SYSTEM FLUSH LOGS part_log;
SELECT 'test5_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_where' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_repl_where;

-- Test 6: control - the same data with the optimization off must produce the same rows.
DROP TABLE IF EXISTS t_ttl_vert_repl_off;

CREATE TABLE t_ttl_vert_repl_off
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
    vertical_merge_optimize_ttl_delete = 0,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_ttl_vert_repl_off
SELECT
    number % 200 AS id,
    intDiv(number, 200) + 1 AS ver,
    if((id < 100) = (ver = 1), now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00') AS d,
    ver, ver, ver
FROM numbers(400);

OPTIMIZE TABLE t_ttl_vert_repl_off FINAL;

SELECT 'test6_count', count() FROM t_ttl_vert_repl_off;
SELECT 'test6_id_range', min(id), max(id) FROM t_ttl_vert_repl_off;
SELECT 'test6_versions', groupUniqArray(ver) FROM t_ttl_vert_repl_off;
SELECT 'test6_cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_repl_off;

SYSTEM FLUSH LOGS part_log;
SELECT 'test6_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_repl_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_repl_off;

-- Test 7: nothing reserves the name of the TTL filter column, so a table may already have one.
-- The merge has to pick a free name instead: reading a user column as the filter would drop live
-- rows, and the duplicate name breaks the merge outright once the column is part of the key.
DROP TABLE IF EXISTS t_ttl_vert_filter_name;

CREATE TABLE t_ttl_vert_filter_name
(
    id UInt64,
    d DateTime,
    _ttl_filter UInt8,
    c1 UInt64
)
ENGINE = MergeTree
ORDER BY (id, _ttl_filter)
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

-- Keys 1 and 2 are live and carry the value the filter treats as "drop"; key 9 is the row the TTL
-- is there to remove, so the merge takes the TTL delete path.
INSERT INTO t_ttl_vert_filter_name VALUES
    (1, '2100-01-01 00:00:00', 0, 101),
    (2, '2100-01-01 00:00:00', 0, 102),
    (9, '2000-01-01 00:00:00', 1, 109);

OPTIMIZE TABLE t_ttl_vert_filter_name FINAL;

SELECT 'test7_rows', id, _ttl_filter, c1 FROM t_ttl_vert_filter_name ORDER BY id;

SYSTEM FLUSH LOGS part_log;
SELECT 'test7_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_filter_name' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_filter_name;
