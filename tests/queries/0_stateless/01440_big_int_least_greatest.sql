SELECT  least(toInt8(127), toInt128(0)) x, least(toInt8(127), toInt128(128)) x2,
        least(toInt8(-128), toInt128(0)) x3, least(toInt8(-128), toInt128(-129)) x4,
        greatest(toInt8(127), toInt128(0)) y, greatest(toInt8(127), toInt128(128)) y2,
        greatest(toInt8(-128), toInt128(0)) y3, greatest(toInt8(-128), toInt128(-129)) y4,
        toTypeName(x), toTypeName(y);

SELECT  least(toInt8(127), toInt256(0)) x, least(toInt8(127), toInt256(128)) x2,
        least(toInt8(-128), toInt256(0)) x3, least(toInt8(-128), toInt256(-129)) x4,
        greatest(toInt8(127), toInt256(0)) y, greatest(toInt8(127), toInt256(128)) y2,
        greatest(toInt8(-128), toInt256(0)) y3, greatest(toInt8(-128), toInt256(-129)) y4,
        toTypeName(x), toTypeName(y);

SELECT  least(toInt64(9223372036854775807), toInt128(0)) x, least(toInt64(9223372036854775807), toInt128('9223372036854775808')) x2,
        least(toInt64(-9223372036854775808), toInt128(0)) x3, least(toInt64(-9223372036854775808), toInt128('-9223372036854775809')) x4,
        greatest(toInt64(9223372036854775807), toInt128(0)) y, greatest(toInt64(9223372036854775807), toInt128('9223372036854775808')) y2,
        greatest(toInt64(-9223372036854775808), toInt128(0)) y3, greatest(toInt64(-9223372036854775808), toInt128('-9223372036854775809')) y4,
        toTypeName(x), toTypeName(y);

SELECT  least(toInt64(9223372036854775807), toInt256(0)) x, least(toInt64(9223372036854775807), toInt256('9223372036854775808')) x2,
        least(toInt64(-9223372036854775808), toInt256(0)) x3, least(toInt64(-9223372036854775808), toInt256('-9223372036854775809')) x4,
        greatest(toInt64(9223372036854775807), toInt256(0)) y, greatest(toInt64(9223372036854775807), toInt256('9223372036854775808')) y2,
        greatest(toInt64(-9223372036854775808), toInt256(0)) y3, greatest(toInt64(-9223372036854775808), toInt256('-9223372036854775809')) y4,
        toTypeName(x), toTypeName(y);

SELECT  least(toUInt8(255), toUInt256(0)) x, least(toUInt8(255), toUInt256(256)) x2,
        greatest(toUInt8(255), toUInt256(0)) y, greatest(toUInt8(255), toUInt256(256)) y2,
        toTypeName(x), toTypeName(y);

SELECT  least(toUInt64('18446744073709551615'), toUInt256(0)) x, least(toUInt64('18446744073709551615'), toUInt256('18446744073709551616')) x2,
        greatest(toUInt64('18446744073709551615'), toUInt256(0)) y, greatest(toUInt64('18446744073709551615'), toUInt256('18446744073709551616')) y2,
        toTypeName(x), toTypeName(y);

SELECT least(toUInt32(0), toInt256(0)), greatest(toInt32(0), toUInt256(0)); -- { serverError 43 }
SELECT least(toInt32(0), toUInt256(0)), greatest(toInt32(0), toUInt256(0)); -- { serverError 43 }
