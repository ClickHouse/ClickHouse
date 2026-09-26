-- intDivOrZero must return 0 (not raise ILLEGAL_DIVISION) when the actual integer division
-- would lead to an FPE, including mixed signed/unsigned operands where the divisor is cast to -1
-- (e.g. Int8(-128) / UInt8(255) is evaluated as Int8(-128) / Int8(-1)).

SELECT 'mixed signed/unsigned where divisor casts to -1 (would FPE -> 0)';
SELECT intDivOrZero(toInt8(-128), toUInt8(255));
SELECT intDivOrZero(materialize(toInt8(-128)), materialize(toUInt8(255)));

SELECT 'mixed signed/unsigned where divisor casts to -1 but no overflow (valid result)';
SELECT intDivOrZero(toInt8(-100), toUInt8(255));
SELECT intDivOrZero(materialize(toInt8(-100)), materialize(toUInt8(255)));

SELECT 'mixed signed/unsigned without cast to -1 (valid result)';
SELECT intDivOrZero(toInt8(-128), toInt16(255));
SELECT intDivOrZero(toInt16(-128), toUInt8(255));

SELECT 'plain FPE cases still return 0';
SELECT intDivOrZero(toInt32(-2147483648), toInt32(-1));
SELECT intDivOrZero(1, 0);
SELECT intDivOrZero(materialize(toInt32(-2147483648)), materialize(toInt32(-1)));
