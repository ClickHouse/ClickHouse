-- Regression test for STID 1751-507c.
-- `countDigits` computed the absolute value with plain unary minus on the signed
-- native integer type of a `Decimal*` column. If the underlying value equals the
-- signed minimum (for example `INT32_MIN` for `Decimal32`), `-value` overflows and
-- is undefined behavior — caught by UBSAN on the arm_asan_ubsan CI build.
--
-- `Decimal*` columns constructed through regular `CAST`/`toDecimalXX` paths cannot
-- hold out-of-precision-range values, so we use `reinterpret` to plant the signed
-- minimum into the underlying native integer and force the negative branch.

-- Integer branch: signed minimums already exercised the `NativeT`-unsigned cast path
-- and must remain stable.
SELECT countDigits(toInt8(-128));
SELECT countDigits(toInt16(-32768));
SELECT countDigits(toInt32(-2147483648));
SELECT countDigits(toInt64(-9223372036854775808));
SELECT countDigits(toInt128('-170141183460469231731687303715884105728'));
SELECT countDigits(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'));

-- Decimal branch: the fuzz-caught UB path. Each `reinterpret` plants the signed
-- minimum into the underlying native integer of a `Decimal*` column.
SELECT countDigits(reinterpret(toInt32(-2147483648), 'Decimal32(0)'));
SELECT countDigits(reinterpret(toInt64(-9223372036854775808), 'Decimal64(0)'));
SELECT countDigits(reinterpret(toInt128('-170141183460469231731687303715884105728'), 'Decimal128(0)'));
SELECT countDigits(reinterpret(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), 'Decimal256(0)'));

-- Non-constant inputs (materialize avoids constant folding and forces the column path).
SELECT countDigits(materialize(reinterpret(toInt32(-2147483648), 'Decimal32(0)')));
SELECT countDigits(materialize(reinterpret(toInt64(-9223372036854775808), 'Decimal64(0)')));
SELECT countDigits(materialize(reinterpret(toInt128('-170141183460469231731687303715884105728'), 'Decimal128(0)')));
SELECT countDigits(materialize(reinterpret(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), 'Decimal256(0)')));
