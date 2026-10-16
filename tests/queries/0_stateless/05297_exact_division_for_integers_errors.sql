SET exact_division_for_integers = 1;
SELECT 1 / 0; -- { serverError ILLEGAL_DIVISION }
SELECT materialize(1) / 0; -- { serverError ILLEGAL_DIVISION }
SELECT 1 / materialize(0); -- { serverError ILLEGAL_DIVISION }
SELECT materialize(1) / materialize(0); -- { serverError ILLEGAL_DIVISION }
SELECT toNullable(materialize(1)) / toNullable(materialize(0)); -- { serverError ILLEGAL_DIVISION }
SELECT toInt8(-128) / toInt8(-1); -- { serverError ILLEGAL_DIVISION }
SELECT materialize(toInt8(-128)) / toInt16(-1); -- { serverError ILLEGAL_DIVISION }
SELECT toInt8(-128) / materialize(toInt256(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT toUInt8(200) / toInt8(1); -- { serverError ILLEGAL_DIVISION }
SELECT materialize(toUInt8(255)) / materialize(toInt8(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT toInt128('-170141183460469231731687303715884105728') / -1; -- { serverError ILLEGAL_DIVISION }
SELECT materialize(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968')) / -1; -- { serverError ILLEGAL_DIVISION }
SELECT toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') / toInt8(1); -- { serverError ILLEGAL_DIVISION }
SELECT materialize(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')) / materialize(toInt8(-1)); -- { serverError ILLEGAL_DIVISION }
