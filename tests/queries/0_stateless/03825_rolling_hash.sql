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
