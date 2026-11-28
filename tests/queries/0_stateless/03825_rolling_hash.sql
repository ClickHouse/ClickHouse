-- Test content-defined chunking (CDC) via Buzhash rolling hash
SELECT '--- contentDefinedChunks';
SELECT contentDefinedChunks('', 4, 1000);
SELECT contentDefinedChunks('ab', 4, 1000);
SELECT contentDefinedChunks('abcdefghijklmnop', 4, 1000);
SELECT arrayStringConcat(contentDefinedChunks('abcdefghijklmnop', 4, 1000), '') = 'abcdefghijklmnop';

SELECT '--- contentDefinedChunkOffsets';
SELECT contentDefinedChunkOffsets('', 4, 1000);
SELECT contentDefinedChunkOffsets('abcdefghijklmnop', 4, 1000);
SELECT length(contentDefinedChunks('abcdefghijklmnop', 4, 1000)) = length(contentDefinedChunkOffsets('abcdefghijklmnop', 4, 1000));

SELECT '--- UTF8 variants';
SELECT contentDefinedChunksUTF8('привет', 2, 1000);
SELECT contentDefinedChunkOffsetsUTF8('привет', 2, 1000);
SELECT length(contentDefinedChunksUTF8('привет', 2, 1000)) = length(contentDefinedChunkOffsetsUTF8('привет', 2, 1000));

SELECT '--- deterministic boundaries';
SELECT contentDefinedChunkOffsets('abcdefghijklmnop', 4, 5);
SELECT contentDefinedChunks('abcdefghijklmnop', 4, 5);
SELECT contentDefinedChunkOffsets('The quick brown fox jumps over the lazy dog', 4, 4);
SELECT contentDefinedChunks('The quick brown fox jumps over the lazy dog', 4, 4);
SELECT contentDefinedChunkOffsetsUTF8('привет', 2, 2);
SELECT contentDefinedChunksUTF8('привет', 2, 2);
SELECT contentDefinedChunkOffsetsUTF8('привет мир', 2, 2);
SELECT contentDefinedChunksUTF8('привет мир', 2, 2);
