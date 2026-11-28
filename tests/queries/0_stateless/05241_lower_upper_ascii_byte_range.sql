-- lower/upper flip exactly [A-Z] / [a-z] and leave every other byte alone, including
-- the bytes directly outside those ranges: @ [ ` {
SELECT lower('@[`{'), upper('@[`{');

-- All 256 byte values at every offset modulo the SIMD block, against a byte-wise reference.
WITH
    arrayStringConcat(arrayMap(b -> char(b), range(256))) AS bytes,
    arrayStringConcat(arrayMap(b -> char(if(b >= 65 AND b <= 90, b + 32, b)), range(256))) AS bytes_lower,
    arrayStringConcat(arrayMap(b -> char(if(b >= 97 AND b <= 122, b - 32, b)), range(256))) AS bytes_upper
SELECT
    countIf(lower(substring(repeat(bytes, 2), number + 1, 256)) != substring(repeat(bytes_lower, 2), number + 1, 256)),
    countIf(upper(substring(repeat(bytes, 2), number + 1, 256)) != substring(repeat(bytes_upper, 2), number + 1, 256))
FROM numbers(64);
