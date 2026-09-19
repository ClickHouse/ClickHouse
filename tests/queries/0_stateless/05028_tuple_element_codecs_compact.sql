-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test forces Compact parts and exercises merge and mutation writers.

DROP TABLE IF EXISTS t_tuple_codec_compact;

SET enable_tuple_element_codecs = 1;

CREATE TABLE t_tuple_codec_compact
(
    key UInt64,
    payload Tuple(
        number UInt64 CODEC(Delta, LZ4),
        text String CODEC(ZSTD(1)),
        inherited UInt64
    ) CODEC(LZ4HC(4))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000;

SYSTEM STOP MERGES t_tuple_codec_compact;

INSERT INTO t_tuple_codec_compact SELECT number, (number, toString(number % 100), number * 2) FROM numbers(1000);
INSERT INTO t_tuple_codec_compact SELECT number + 1000, (number + 1000, toString(number % 100), (number + 1000) * 2) FROM numbers(1000);

SYSTEM START MERGES t_tuple_codec_compact;
OPTIMIZE TABLE t_tuple_codec_compact FINAL;

SELECT count(), groupUniqArray(part_type)
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_compact' AND active;

ALTER TABLE t_tuple_codec_compact
    UPDATE payload = tuple(
        payload.number + if(key % 2 = 0, 1, 0),
        payload.text,
        payload.inherited + if(key % 2 = 0, 1, 0))
    WHERE 1
SETTINGS mutations_sync = 2;

SELECT
    countIf(payload.number = key + if(key % 2 = 0, 1, 0)) = 2000,
    countIf(payload.inherited = key * 2 + if(key % 2 = 0, 1, 0)) = 2000
FROM t_tuple_codec_compact;

DROP TABLE t_tuple_codec_compact;
