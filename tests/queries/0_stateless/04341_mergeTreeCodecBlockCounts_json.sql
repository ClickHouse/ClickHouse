-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Compact, where codec_block_counts is empty.

DROP TABLE IF EXISTS t_json_codec;

-- A JSON serialises its typed paths as separate dynamic substreams (no static subcolumns), so each gets its own per-stream codec counts.
CREATE TABLE t_json_codec (j JSON)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_json_codec SELECT '{"a":' || toString(number) || ',"b":' || toString(number * 2) || '}'
FROM numbers(100000);

SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_json_codec) ORDER BY substream;

DROP TABLE t_json_codec;
