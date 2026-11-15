-- A constant negative divisor is converted to its positive counterpart for the optimized path.
-- The minimum value of a signed type has no representable positive counterpart, so it must use
-- the general modulo implementation instead. A materialized divisor exercises that implementation
-- directly and serves as the control result.

SELECT modulo(materialize(toInt32(5)), toInt8(-128)), modulo(materialize(toInt32(5)), materialize(toInt8(-128)));
SELECT modulo(materialize(toInt32(-5)), toInt8(-128)), modulo(materialize(toInt32(-5)), materialize(toInt8(-128)));
SELECT modulo(materialize(toInt32(-128)), toInt8(-128)), modulo(materialize(toInt32(-128)), materialize(toInt8(-128)));

SELECT modulo(materialize(toInt32(5)), toInt16(-32768)), modulo(materialize(toInt32(5)), materialize(toInt16(-32768)));
SELECT modulo(materialize(toInt32(5)), toInt32(-2147483648)), modulo(materialize(toInt32(5)), materialize(toInt32(-2147483648)));
SELECT modulo(materialize(toInt64(5)), toInt64(-9223372036854775808)), modulo(materialize(toInt64(5)), materialize(toInt64(-9223372036854775808)));

SELECT modulo(materialize(toInt32(-2147483648)), toInt32(-2147483648)), modulo(materialize(toInt32(-2147483648)), materialize(toInt32(-2147483648)));
SELECT modulo(materialize(toInt64(-9223372036854775808)), toInt64(-9223372036854775808)), modulo(materialize(toInt64(-9223372036854775808)), materialize(toInt64(-9223372036854775808)));
