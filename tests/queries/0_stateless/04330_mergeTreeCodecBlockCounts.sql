-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Compact, where the codec maps are empty.

DROP TABLE IF EXISTS t_codec_func;
DROP TABLE IF EXISTS t_not_mergetree;

-- Wrong number of arguments.
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase()); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Not a MergeTree table.
CREATE TABLE t_not_mergetree (a UInt64) ENGINE = Memory;
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_not_mergetree); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_not_mergetree;

CREATE TABLE t_codec_func (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_codec_func SELECT number FROM numbers(100000);

SELECT column, mapContains(codec_block_counts, 'LZ4') AS has_lz4
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_func) ORDER BY column;

SELECT column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_func) ORDER BY column;

DROP TABLE t_codec_func;
