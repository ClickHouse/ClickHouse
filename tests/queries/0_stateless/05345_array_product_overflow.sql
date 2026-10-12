-- arrayProduct returns Float64, so an integer product that does not fit the integer type must not wrap.
-- A lambda returning a constant is multiplied by a separate code path, so both paths are checked.

SELECT 'Integer product beyond the integer range';
SELECT arrayProduct([toInt64(1000000000000000000), toInt64(1000000000000000000)]);
SELECT arrayProduct([toInt64(-1000000000000000000), toInt64(1000000000000000000)]);
SELECT arrayProduct(materialize([toUInt64(4294967296), toUInt64(4294967296)]));
SELECT arrayProduct(materialize([toInt32(2000000000), toInt32(2000000000), toInt32(2000000000)]));
SELECT arrayProduct([toInt128('100000000000000000000'), toInt128('100000000000000000000')]);
SELECT arrayProduct(materialize([toUInt256(pow(2, 200)), toUInt256(pow(2, 200))]));
SELECT arrayProduct(x -> toInt64(1000000000000000000), [1, 2]);
SELECT arrayProduct(x -> toInt128('100000000000000000000'), [1, 2]);

SELECT 'Integer product within the integer range';
SELECT arrayProduct([toInt64(3037000499), toInt64(3037000499)]);
SELECT arrayProduct(materialize([toInt64(-3037000499), toInt64(3037000499)]));
SELECT arrayProduct(x -> toInt64(3037000499), [1, 2]);

SELECT 'A zero element after the integer range is exceeded';
SELECT arrayProduct(materialize([toInt64(-1000000000000000000), toInt64(1000000000000000000), toInt64(0)]));
SELECT arrayProduct(materialize([toUInt256(pow(2, 255)), toUInt256(pow(2, 255)), toUInt256(pow(2, 255)), toUInt256(pow(2, 255)), toUInt256(pow(2, 255)), toUInt256(0)]));

SELECT 'Decimal product beyond the accumulator range';
SELECT arrayProduct([toDecimal128('1000000000000000000000000000000', 0), toDecimal128('1000000000000000000000000000000', 0)]); -- { serverError DECIMAL_OVERFLOW }
SELECT arrayProduct(materialize([toDecimal64('100000000000000000', 0), toDecimal64('100000000000000000', 0), toDecimal64('100000000000000000', 0)])); -- { serverError DECIMAL_OVERFLOW }
SELECT arrayProduct(x -> toDecimal128('1000000000000000000000000000000', 0), [1, 2]); -- { serverError DECIMAL_OVERFLOW }
