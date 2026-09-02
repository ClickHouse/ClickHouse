-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test fixes the part format, forces a vertical Wide merge,
-- and checks the first-writer codec of a shared offsets stream.

DROP TABLE IF EXISTS t_tuple_codec_shared_nested_wide;
DROP TABLE IF EXISTS t_tuple_codec_shared_nested_compact;

CREATE TABLE t_tuple_codec_shared_nested_wide
(
    id UInt64,
    n Nested(a UInt64, b String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

-- This is legal legacy metadata: the flattened Array columns have independent codecs even
-- though both serializations reach the same n.size0 offsets stream.
ALTER TABLE t_tuple_codec_shared_nested_wide MODIFY COLUMN `n.a` CODEC(ZSTD(3));
ALTER TABLE t_tuple_codec_shared_nested_wide MODIFY COLUMN `n.b` CODEC(LZ4HC(4));

SYSTEM STOP MERGES t_tuple_codec_shared_nested_wide;

INSERT INTO t_tuple_codec_shared_nested_wide
SELECT
    number,
    [number, number + 1] AS `n.a`,
    [toString(number % 10), toString((number + 1) % 10)] AS `n.b`
FROM numbers(1000);

INSERT INTO t_tuple_codec_shared_nested_wide
SELECT
    number + 1000,
    [number + 1000, number + 1001] AS `n.a`,
    [toString(number % 10), toString((number + 1) % 10)] AS `n.b`
FROM numbers(1000);

SELECT count() = 2, groupUniqArray(part_type) = ['Wide']
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_shared_nested_wide' AND active;

SELECT count() = 2000, sum(length(n.a)) = 4000, countIf(length(n.a) != length(n.b)) = 0
FROM t_tuple_codec_shared_nested_wide;

SYSTEM START MERGES t_tuple_codec_shared_nested_wide;
OPTIMIZE TABLE t_tuple_codec_shared_nested_wide FINAL;

SELECT count() = 1, groupUniqArray(part_type) = ['Wide']
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_shared_nested_wide' AND active;

-- The value streams retain their independent codecs, while schema order makes n.a the first
-- owner of the shared n.size0 stream.
SELECT DISTINCT substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_shared_nested_wide)
WHERE substream IN ('n.a', 'n.b', 'n.size0')
ORDER BY substream;

CREATE TABLE t_tuple_codec_shared_nested_compact
(
    id UInt64,
    n Nested(a UInt64, b String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000;

ALTER TABLE t_tuple_codec_shared_nested_compact MODIFY COLUMN `n.a` CODEC(ZSTD(3));
ALTER TABLE t_tuple_codec_shared_nested_compact MODIFY COLUMN `n.b` CODEC(LZ4HC(4));

SYSTEM STOP MERGES t_tuple_codec_shared_nested_compact;

INSERT INTO t_tuple_codec_shared_nested_compact
SELECT
    number,
    [number, number + 1] AS `n.a`,
    [toString(number % 10), toString((number + 1) % 10)] AS `n.b`
FROM numbers(1000);

INSERT INTO t_tuple_codec_shared_nested_compact
SELECT
    number + 1000,
    [number + 1000, number + 1001] AS `n.a`,
    [toString(number % 10), toString((number + 1) % 10)] AS `n.b`
FROM numbers(1000);

SELECT count() = 2, groupUniqArray(part_type) = ['Compact']
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_shared_nested_compact' AND active;

SELECT count() = 2000, sum(length(n.a)) = 4000, countIf(length(n.a) != length(n.b)) = 0
FROM t_tuple_codec_shared_nested_compact;

SYSTEM START MERGES t_tuple_codec_shared_nested_compact;
OPTIMIZE TABLE t_tuple_codec_shared_nested_compact FINAL;

SELECT count() = 1, groupUniqArray(part_type) = ['Compact']
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_shared_nested_compact' AND active;

SELECT count() = 2000, sum(length(n.a)) = 4000, countIf(length(n.a) != length(n.b)) = 0
FROM t_tuple_codec_shared_nested_compact;

DROP TABLE t_tuple_codec_shared_nested_wide;
DROP TABLE t_tuple_codec_shared_nested_compact;
