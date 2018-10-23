SELECT sipHash64(1, 2, 3);
SELECT sipHash64(1, 3, 2);
SELECT sipHash64('a', [1, 2, 3], 4);

SELECT murmurHash2_32(1, 2, 3);
SELECT murmurHash2_32(1, 3, 2);
SELECT murmurHash2_32('a', [1, 2, 3], 4);

SELECT murmurHash2_64(1, 2, 3);
SELECT murmurHash2_64(1, 3, 2);
SELECT murmurHash2_64('a', [1, 2, 3], 4);

SELECT murmurHash3_64(1, 2, 3);
SELECT murmurHash3_64(1, 3, 2);
SELECT murmurHash3_64('a', [1, 2, 3], 4);
