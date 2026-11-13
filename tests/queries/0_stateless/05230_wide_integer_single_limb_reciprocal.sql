-- The middle limb needs the second reciprocal correction for this divisor.
WITH
    bitShiftLeft(toUInt256(9223372036854775808), 128)
        + bitShiftLeft(toUInt256(18446744073709551612), 64) AS n,
    toUInt256(9223372036854775810) AS d
SELECT intDiv(n, d), modulo(n, d);

WITH
    bitShiftLeft(toUInt256(9223372036854775808), 128)
        + bitShiftLeft(toUInt256(18446744073709551612), 64) + toUInt256(1) AS n,
    toUInt256(9223372036854775810) AS d
SELECT intDiv(n, d), modulo(n, d);

WITH
    -toInt256(bitShiftLeft(toUInt256(9223372036854775808), 128)
        + bitShiftLeft(toUInt256(18446744073709551612), 64) + toUInt256(1)) AS n,
    toInt256(9223372036854775810) AS d
SELECT intDiv(n, d), modulo(n, d);

WITH
    -toInt256(bitShiftLeft(toUInt256(9223372036854775808), 128)
        + bitShiftLeft(toUInt256(18446744073709551612), 64) + toUInt256(1)) AS n,
    -toInt256(9223372036854775810) AS d
SELECT intDiv(n, d), modulo(n, d);

WITH toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') AS n
SELECT intDiv(n, toUInt256(10)), modulo(n, toUInt256(10));

-- Zero lower limbs exercise the first reciprocal correction.
WITH bitShiftLeft(toUInt256(1), 192) AS n
SELECT intDiv(n, toUInt256(1)), modulo(n, toUInt256(1));

SELECT toDecimal256('3138550867693340382258177078524771671440765353368947261440', 0)
    / toDecimal256('9223372036854775810', 0);
