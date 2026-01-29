-- Tags: no-fasttest

SELECT sipHash64(1, 2, 3);
SELECT sipHash64(1, 3, 2);
SELECT sipHash64(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));

SELECT hex(sipHash128('foo'));
SELECT hex(sipHash128('\x01'));
SELECT hex(sipHash128('foo', 'foo'));
SELECT hex(sipHash128('foo', 'foo', 'foo'));
SELECT hex(sipHash128(1, 2, 3));

SELECT halfMD5(1, 2, 3);
SELECT halfMD5(1, 3, 2);
SELECT halfMD5(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));

SELECT murmurHash2_32(1, 2, 3);
SELECT murmurHash2_32(1, 3, 2);
SELECT murmurHash2_32(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], (1, 2))));

SELECT murmurHash2_64(1, 2, 3);
SELECT murmurHash2_64(1, 3, 2);
SELECT murmurHash2_64(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));

SELECT murmurHash3_64(1, 2, 3);
SELECT murmurHash3_64(1, 3, 2);
SELECT murmurHash3_64(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));

SELECT hex(murmurHash3_128('foo', 'foo'));
SELECT hex(murmurHash3_128('foo', 'foo', 'foo'));

SELECT gccMurmurHash(1, 2, 3);
SELECT gccMurmurHash(1, 3, 2);
SELECT gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));