-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the output enumerates the physical representation, which many randomised settings change
DROP TABLE IF EXISTS t_patch;

CREATE TABLE t_patch (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_patch;

INSERT INTO t_patch SELECT number, number FROM numbers(100000);

-- A lightweight update leaves an active patch part until a merge applies it.
UPDATE t_patch SET b = 42 WHERE a < 10;

SELECT 'all rows';
SELECT part_name, column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_patch)
ORDER BY part_name, column, substream;

SELECT 'where column = b, both the regular and the patch part';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_patch) WHERE column = 'b' ORDER BY part_name;

SELECT 'patch part substream sizes';
SELECT part_name, substream, data_uncompressed_bytes
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_patch)
WHERE part_name LIKE 'patch-%'
ORDER BY substream;

DROP TABLE t_patch;
