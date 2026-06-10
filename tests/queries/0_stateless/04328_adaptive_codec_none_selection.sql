-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads system.parts_columns, which randomised enable_block_number/offset_column add rows to.

DROP TABLE IF EXISTS t_adaptive_none;
DROP TABLE IF EXISTS t_adaptive_none_tiny;

-- x: pseudo-random data nothing can shrink, every block must be stored raw (NONE).
-- y: monotonic data, T64 wins, NONE must not appear.
-- z: a short pattern of full-range values repeated, LZ4 must win.
CREATE TABLE t_adaptive_none
(
    x UInt64,
    y UInt64,
    z UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

INSERT INTO t_adaptive_none SELECT cityHash64(number), number, arrayElement([0, 18446744073709551615, 81985529216486895], number % 3 + 1) FROM numbers(1000000);
INSERT INTO t_adaptive_none SELECT cityHash64(number), number, arrayElement([0, 18446744073709551615, 81985529216486895], number % 3 + 1) FROM numbers(1000000, 1000000);
OPTIMIZE TABLE t_adaptive_none FINAL;

SELECT column,
       mapContains(codec_block_counts, 'NONE') AS has_none,
       mapContains(codec_block_counts, 'T64') AS has_t64,
       mapContains(codec_block_counts, 'LZ4') AS has_lz4
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_none' AND active ORDER BY column;

-- A merged part so small that one raw block beats any codec's framing.
CREATE TABLE t_adaptive_none_tiny (x UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

INSERT INTO t_adaptive_none_tiny SELECT cityHash64(number) FROM numbers(4);
INSERT INTO t_adaptive_none_tiny SELECT cityHash64(number) FROM numbers(4, 4);
OPTIMIZE TABLE t_adaptive_none_tiny FINAL;

SELECT 'tiny', column, codec_block_counts
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_none_tiny' AND active ORDER BY column;

SELECT 'read', count(), countIf(x = cityHash64(y)) FROM t_adaptive_none;
CHECK TABLE t_adaptive_none SETTINGS check_query_single_value_result = 1;

DROP TABLE t_adaptive_none;
DROP TABLE t_adaptive_none_tiny;
