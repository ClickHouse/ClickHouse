-- Tags: no-fasttest

SELECT sipHash64(1, 2, 3);
SELECT sipHash64(1, 3, 2);
SELECT sipHash64(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));

SELECT hex(sipHash128('foo')) = hex(reverse(unhex('CC45107CC4B79F62D831BEF2103C7CBF'))) or hex(sipHash128('foo')) = 'CC45107CC4B79F62D831BEF2103C7CBF';
SELECT hex(sipHash128('\x01')) = hex(reverse(unhex('DF2EC2F0669B000EDFF6ADEE264E7D68'))) or hex(sipHash128('\x01')) = 'DF2EC2F0669B000EDFF6ADEE264E7D68';
SELECT hex(sipHash128('foo', 'foo')) = hex(reverse(unhex('4CD1C30C38AB935D418B5269EF197B9E'))) or hex(sipHash128('foo', 'foo')) = '4CD1C30C38AB935D418B5269EF197B9E';
SELECT hex(sipHash128('foo', 'foo', 'foo')) = hex(reverse(unhex('9D78134EE48654D753CCA1B76185CF8E'))) or hex(sipHash128('foo', 'foo', 'foo')) = '9D78134EE48654D753CCA1B76185CF8E';
SELECT hex(sipHash128(1, 2, 3)) = hex(reverse(unhex('389D16428D2AADEC9713905572F42864'))) or hex(sipHash128(1, 2, 3)) = '389D16428D2AADEC9713905572F42864';

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

SELECT hex(murmurHash3_128('foo', 'foo')) = hex(reverse(unhex('8DD5527CC43D76F4760D26BE0F641F7E'))) or hex(murmurHash3_128('foo', 'foo')) = '8DD5527CC43D76F4760D26BE0F641F7E';
SELECT hex(murmurHash3_128('foo', 'foo', 'foo')) = hex(reverse(unhex('F8F7AD9B6CD4CF117A71E277E2EC2931'))) or hex(murmurHash3_128('foo', 'foo', 'foo')) = 'F8F7AD9B6CD4CF117A71E277E2EC2931';

SELECT gccMurmurHash(1, 2, 3);
SELECT gccMurmurHash(1, 3, 2);
SELECT gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2))));