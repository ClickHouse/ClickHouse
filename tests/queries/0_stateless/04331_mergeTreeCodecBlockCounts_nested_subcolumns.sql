-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Compact, where the per-subcolumn files don't exist.

DROP TABLE IF EXISTS t_nested_subcolumns;

-- t.b is an intermediate node (Tuple) with no `.bin` of its own. mergeTreeCodecBlockCounts must list only the final, file-backed subcolumns.
CREATE TABLE t_nested_subcolumns (t Tuple(a UInt32, b Tuple(c UInt32, d UInt32)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_nested_subcolumns SELECT (number, (number, number)) FROM numbers(1000);

SELECT `subcolumns.names`, arrayMap(m -> mapKeys(m), `subcolumns.codec_block_counts`) AS subcolumn_codecs
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_nested_subcolumns) WHERE column = 't';

DROP TABLE t_nested_subcolumns;
