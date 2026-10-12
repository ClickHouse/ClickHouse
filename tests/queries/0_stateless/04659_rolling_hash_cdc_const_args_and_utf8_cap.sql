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

-- The UTF-8 variants reject malformed input, preserving their whole-code-point contract.
SELECT '--- malformed UTF-8 is rejected';
SELECT contentDefinedChunksUTF8(unhex('418042'), 1, 941); -- { serverError BAD_ARGUMENTS }
SELECT contentDefinedChunkOffsetsUTF8(unhex('418042'), 1, 941); -- { serverError BAD_ARGUMENTS }
