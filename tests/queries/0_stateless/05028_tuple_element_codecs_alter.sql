-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test compares codec headers in distinct Wide parts.

DROP TABLE IF EXISTS t_tuple_codec_alter;
DROP TABLE IF EXISTS t_tuple_codec_alter_positional;
DROP TABLE IF EXISTS t_tuple_codec_alter_removed_path;

CREATE TABLE t_tuple_codec_alter
(
    key UInt64,
    payload Tuple(
        kept UInt64 CODEC(ZSTD(1)),
        changed String,
        nested Tuple(
            removed UInt64 CODEC(T64, LZ4),
            defaulted UInt64
        )
    ) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(2)';

SYSTEM STOP MERGES t_tuple_codec_alter;

INSERT INTO t_tuple_codec_alter
SELECT number, (number, toString(number), (number * 2, number + 1))
FROM numbers(100000);

ALTER TABLE t_tuple_codec_alter
    MODIFY COLUMN payload Tuple(
        kept UInt64,
        changed String CODEC(ZSTD(3)),
        nested Tuple(
            removed UInt64 REMOVE CODEC,
            defaulted UInt64 CODEC(Default)
        )
    ) CODEC(LZ4HC(4));

SELECT
    countSubstrings(compression_codec, 'CODEC(') = 4,
    position(compression_codec, 'kept UInt64 CODEC(ZSTD(1))') > 0,
    position(compression_codec, 'changed String CODEC(ZSTD(3))') > 0,
    position(compression_codec, 'removed UInt64 CODEC') = 0,
    position(compression_codec, 'defaulted UInt64 CODEC(Default)') > 0,
    endsWith(compression_codec, 'CODEC(LZ4HC(4))')
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_alter' AND name = 'payload';

INSERT INTO t_tuple_codec_alter
SELECT number + 100000, (number + 100000, toString(number + 100000), ((number + 100000) * 2, number + 100001))
FROM numbers(100000);

-- The first part keeps the old policy; the second part uses the patched metadata.
SELECT
    part_name,
    substream,
    mapKeys(codec_block_counts) AS codecs
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_alter)
WHERE column = 'payload'
ORDER BY part_name, substream;

-- A typed restatement with no codec operations preserves all declarations on valid paths.
ALTER TABLE t_tuple_codec_alter
    MODIFY COLUMN payload Tuple(
        kept UInt64,
        changed String,
        nested Tuple(removed UInt64, defaulted UInt64)
    );

SELECT
    position(type, 'kept UInt64') > 0,
    position(compression_codec, 'kept UInt64 CODEC(ZSTD(1))') > 0,
    countSubstrings(compression_codec, 'CODEC(') = 4
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_alter' AND name = 'payload';

-- Removing a literal declaration exposes inheritance. Removing it twice is an error.
ALTER TABLE t_tuple_codec_alter
    MODIFY COLUMN payload Tuple(
        kept UInt64,
        changed String REMOVE CODEC,
        nested Tuple(removed UInt64, defaulted UInt64)
    );

ALTER TABLE t_tuple_codec_alter
    MODIFY COLUMN payload Tuple(
        kept UInt64,
        changed String REMOVE CODEC,
        nested Tuple(removed UInt64, defaulted UInt64)
    ); -- { serverError BAD_ARGUMENTS }

SELECT
    countSubstrings(compression_codec, 'CODEC(') = 3,
    position(compression_codec, 'changed String CODEC') = 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_alter' AND name = 'payload';

-- Existing root-only forms do not remove element declarations.
ALTER TABLE t_tuple_codec_alter MODIFY COLUMN payload CODEC(ZSTD(4));
ALTER TABLE t_tuple_codec_alter MODIFY COLUMN payload REMOVE CODEC;

SELECT
    countSubstrings(compression_codec, 'CODEC(') = 2,
    position(compression_codec, 'kept UInt64 CODEC(ZSTD(1))') > 0,
    position(compression_codec, 'defaulted UInt64 CODEC(Default)') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_alter' AND name = 'payload';

SYSTEM START MERGES t_tuple_codec_alter;
OPTIMIZE TABLE t_tuple_codec_alter FINAL;

SELECT substream, mapKeys(codec_block_counts) AS codecs
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_alter)
WHERE column = 'payload'
ORDER BY substream;

-- Positional correspondence lets an element be renamed while removing its old declaration.
CREATE TABLE t_tuple_codec_alter_positional
(
    pair Tuple(UInt64 CODEC(T64, LZ4), String)
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_alter_positional
    MODIFY COLUMN pair Tuple(first UInt64 REMOVE CODEC, second String);

SELECT type, compression_codec = ''
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_alter_positional' AND name = 'pair';

-- A retained declaration cannot silently become a dangling path after a type edit.
CREATE TABLE t_tuple_codec_alter_removed_path
(
    payload Tuple(retained UInt64 CODEC(LZ4), other String)
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_alter_removed_path
    MODIFY COLUMN payload Tuple(other String); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_tuple_codec_alter_removed_path;
DROP TABLE t_tuple_codec_alter_positional;
DROP TABLE t_tuple_codec_alter;
