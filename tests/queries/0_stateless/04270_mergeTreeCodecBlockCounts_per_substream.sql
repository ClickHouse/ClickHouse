-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings would change max_compress_block_size and skew the per-substream block counts.

-- Each tuple element is its own stream, sized so every stream gets a distinct block count. Verify the count is tied to the right substream.

DROP TABLE IF EXISTS t_order;

CREATE TABLE t_order
(
    t Tuple(
        o FixedString(15),
        e FixedString(5),
        l FixedString(12),
        b FixedString(2),
        n FixedString(14),
        c FixedString(3),
        k FixedString(11),
        d FixedString(4),
        m FixedString(13),
        a FixedString(1),
        i FixedString(9),
        j FixedString(10),
        f FixedString(6),
        h FixedString(8),
        g FixedString(7)
    ) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t_order
SELECT (
    toFixedString(repeat('x', 15), 15),
    toFixedString(repeat('x',  5),  5),
    toFixedString(repeat('x', 12), 12),
    toFixedString(repeat('x',  2),  2),
    toFixedString(repeat('x', 14), 14),
    toFixedString(repeat('x',  3),  3),
    toFixedString(repeat('x', 11), 11),
    toFixedString(repeat('x',  4),  4),
    toFixedString(repeat('x', 13), 13),
    toFixedString(repeat('x',  1),  1),
    toFixedString(repeat('x',  9),  9),
    toFixedString(repeat('x', 10), 10),
    toFixedString(repeat('x',  6),  6),
    toFixedString(repeat('x',  8),  8),
    toFixedString(repeat('x',  7),  7)
)
FROM numbers(100000);

SELECT
    substream,
    codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_order)
WHERE column = 't'
ORDER BY substream;

DROP TABLE t_order;
