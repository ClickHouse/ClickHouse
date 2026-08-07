-- Content-defined chunking: `window_size` and `reverse_probability` are const-only arguments.
SELECT '--- non-constant parameters are rejected';
SELECT contentDefinedChunks('abcdefghijklmnop', materialize(4), 1000); -- { serverError ILLEGAL_COLUMN }
SELECT contentDefinedChunks('abcdefghijklmnop', 4, materialize(1000)); -- { serverError ILLEGAL_COLUMN }
SELECT contentDefinedChunksUTF8('abcdefghijklmnop', materialize(4), 1000); -- { serverError ILLEGAL_COLUMN }
SELECT contentDefinedChunkOffsets('abcdefghijklmnop', 4, materialize(1000)); -- { serverError ILLEGAL_COLUMN }
SELECT contentDefinedChunkOffsetsUTF8('abcdefghijklmnop', materialize(4), 1000); -- { serverError ILLEGAL_COLUMN }
SELECT contentDefinedChunks(s, w, p) FROM (SELECT 'abcdefghijklmnop' AS s, materialize(4) AS w, 1000 AS p); -- { serverError ILLEGAL_COLUMN }

-- Wide unsigned integers are rejected instead of being silently narrowed to 64 bits
-- (`toUInt128(18446744073709551617)` would otherwise wrap to 1 and pass the range checks).
SELECT '--- wide unsigned integer parameters are rejected';
SELECT contentDefinedChunks('abcdefghijklmnop', toUInt128(18446744073709551617), 1000); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contentDefinedChunks('abcdefghijklmnop', 4, toUInt256(18446744073709551618)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contentDefinedChunksUTF8('abcdefghijklmnop', toUInt128(4), 1000); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contentDefinedChunkOffsets('abcdefghijklmnop', 4, toUInt256(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contentDefinedChunkOffsetsUTF8('abcdefghijklmnop', toUInt128(4), 1000); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The max chunk size cap holds even for malformed UTF-8 (a run of continuation bytes longer than the cap).
-- reverse_probability = 2 gives the minimal max chunk size of 262144 bytes.
SELECT '--- strict max chunk size cap on malformed UTF-8';
WITH
    concat('A', repeat(char(0x80), 300000), 'B') AS s,
    contentDefinedChunkOffsetsUTF8(s, 8, 2) AS offs
SELECT
    arrayMax(arrayDifference(arrayPushBack(offs, toUInt64(length(s))))) <= 262144,
    arrayStringConcat(contentDefinedChunksUTF8(s, 8, 2), '') = s;
