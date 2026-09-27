-- Non-constant inputs whose lengths cross the 16, 32 and 64 byte SIMD block sizes,
-- with the offending byte at every possible position, to exercise the vectorized
-- kernel, its scalar tail and the per-row boundaries.

SELECT 'all ascii, every length up to 300';
SELECT count(), countIf(isValidASCII(repeat('a', number)) = 1) FROM numbers(300);

SELECT 'one high byte at every position, every length up to 200';
SELECT count(), countIf(isValidASCII(concat(repeat('a', pos), '\x80', repeat('a', len - pos - 1))) = 0)
FROM (SELECT number AS len FROM numbers(1, 200)) ARRAY JOIN range(len) AS pos;

SELECT count(), countIf(isValidASCII(concat(repeat('a', pos), '\xFF', repeat('a', len - pos - 1))) = 0)
FROM (SELECT number AS len FROM numbers(1, 200)) ARRAY JOIN range(len) AS pos;

SELECT 'boundary bytes 0x00 and 0x7F are ascii at every position';
SELECT count(), countIf(isValidASCII(concat(repeat('a', pos), '\x00', repeat('\x7F', len - pos - 1))) = 1)
FROM (SELECT number AS len FROM numbers(1, 200)) ARRAY JOIN range(len) AS pos;

SELECT 'per-row results do not bleed into neighbours';
SELECT isValidASCII(if(number % 3 = 0, repeat('a', 100), concat(repeat('a', 99), 'é'))) FROM numbers(9);
SELECT isValidASCII(if(number % 2 = 0, '', 'ä')) FROM numbers(6);

SELECT 'fixed string';
SELECT count(), countIf(isValidASCII(toFixedString(concat(repeat('a', number), '\x80', repeat('a', 63 - number)), 64)) = 0) FROM numbers(64);
SELECT count(), countIf(isValidASCII(toFixedString(repeat('a', 64), 64)) = 1) FROM numbers(10);
SELECT isValidASCII(toFixedString(materialize('ab'), 64));
SELECT isValidASCII(toFixedString(materialize('a\x80'), 64));
