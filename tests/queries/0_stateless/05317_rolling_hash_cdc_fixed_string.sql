-- Content-defined chunking on `FixedString` input, including embedded zero bytes and high-bit bytes.

DROP TABLE IF EXISTS t_cdc_fixed;
CREATE TABLE t_cdc_fixed (id UInt32, str String, s FixedString(16) MATERIALIZED str) ENGINE = Memory;
-- Every `str` is exactly 16 bytes long, so `s` holds the same bytes, including trailing zero bytes.
INSERT INTO t_cdc_fixed (id, str) VALUES (1, 'abcdefghijklmnop'), (2, 'ab\0cd\xFF\xFEefgh\0\0ijk'), (3, concat('abc', repeat('\0', 13)));

-- `FixedString` must produce the same result as the equivalent `String`.
SELECT id,
    length(str),
    contentDefinedChunkOffsets(s, 4, 5),
    contentDefinedChunkOffsets(s, 4, 5) = contentDefinedChunkOffsets(str, 4, 5),
    contentDefinedChunks(s, 4, 5) = contentDefinedChunks(str, 4, 5),
    arrayStringConcat(contentDefinedChunks(s, 4, 5), '') = str,
    contentDefinedChunkOffsets(s, 2, 3) = contentDefinedChunkOffsets(str, 2, 3),
    contentDefinedChunks(s, 2, 3) = contentDefinedChunks(str, 2, 3)
FROM t_cdc_fixed ORDER BY id;

SELECT hex(arrayJoin(contentDefinedChunks(s, 2, 3))) FROM t_cdc_fixed WHERE id = 2;

-- UTF-8 variants on valid UTF-8 `FixedString` (no padding).
SELECT contentDefinedChunkOffsetsUTF8(toFixedString('привет', 12), 2, 2), contentDefinedChunksUTF8(toFixedString('привет', 12), 2, 2);

DROP TABLE t_cdc_fixed;
