-- Printing a 256-bit integer takes a different path once the value no longer fits in 128 bits, and
-- above that it strips 18-digit blocks off the value with a division by 10^18 each. The boundaries
-- worth covering are therefore the powers of two around 2^128, the powers of ten around 10^18 and
-- the extremes of the types.

SELECT toString(toUInt256(0));
SELECT toString(toUInt256('340282366920938463463374607431768211455')); -- 2^128 - 1
SELECT toString(toUInt256('340282366920938463463374607431768211456')); -- 2^128
SELECT toString(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')); -- 2^256 - 1
SELECT toString(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968')); -- -2^255
SELECT toString(toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')); -- 2^255 - 1
SELECT toString(toUInt256('999999999999999999')); -- 10^18 - 1
SELECT toString(toUInt256('1000000000000000000')); -- 10^18
SELECT toString(toUInt256('999999999999999999999999999999999999')); -- 10^36 - 1
SELECT toString(toUInt256('1000000000000000000000000000000000000')); -- 10^36

-- Every power of two and its neighbours, printed and read back.
SELECT count() FROM
(
    SELECT bitShiftLeft(toUInt256(1), number) AS p
    FROM numbers(256)
)
WHERE toUInt256(toString(p)) != p OR toUInt256(toString(p - 1)) != p - 1;

-- The same for the signed type, in both signs.
SELECT count() FROM
(
    SELECT toInt256(bitShiftLeft(toUInt256(1), number)) AS p
    FROM numbers(255)
)
WHERE toInt256(toString(p)) != p OR toInt256(toString(-p)) != -p;

-- Every power of ten that fits, and the value just below it. The neighbour is spelled out as a
-- string rather than as `p - 1`, because subtracting from a `UInt256` yields an `Int256`, which
-- wraps for the powers of ten above 2^255.
SELECT count() FROM
(
    SELECT toUInt256(concat('1', repeat('0', number))) AS p,
           toUInt256(if(number = 0, '0', repeat('9', number))) AS below
    FROM numbers(78)
)
WHERE toUInt256(toString(p)) != p OR toUInt256(toString(below)) != below;

-- Values spread over the whole range, at every width. No printed value may carry a leading zero,
-- which is how a wrongly sized digit block would show up.
SELECT count() FROM
(
    SELECT bitShiftRight(
        bitShiftLeft(toUInt256(cityHash64(number, 1)), 192) + bitShiftLeft(toUInt256(cityHash64(number, 2)), 128)
            + bitShiftLeft(toUInt256(cityHash64(number, 3)), 64) + cityHash64(number, 4),
        number % 256) AS p
    FROM numbers(10000)
)
WHERE toUInt256(toString(p)) != p OR (length(toString(p)) > 1 AND startsWith(toString(p), '0'));

-- Formatting a Decimal256 prints the whole part as a 256-bit integer and then divides the
-- fractional part by ten once per digit.
SELECT toString(toDecimal256('0.0000000000000000000000000000000000000001', 40));
SELECT toString(toDecimal256('-0.0000000000000000000000000000000000000001', 40));
SELECT toString(toDecimal256('12345678901234567890123456789012345.6789012345678901234567890123456789012345', 40));
SELECT count() FROM
(
    SELECT toDecimal256(number, 40) / 7 AS d
    FROM numbers(10000)
)
WHERE toDecimal256(toString(d), 40) != d;
