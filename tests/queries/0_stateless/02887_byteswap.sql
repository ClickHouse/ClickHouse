SELECT byteSwap(0::UInt8);
SELECT byteSwap(1::UInt8);
SELECT byteSwap(255::UInt8);

SELECT byteSwap(256::UInt16);
SELECT byteSwap(4135::UInt16);
SELECT byteSwap(10000::UInt16);
SELECT byteSwap(65535::UInt16);

SELECT byteSwap(65536::UInt32);
SELECT byteSwap(3351772109::UInt32);
SELECT byteSwap(3455829959::UInt32);
SELECT byteSwap(4294967295::UInt32);

SELECT byteSwap(4294967296::UInt64);
SELECT byteSwap(123294967295::UInt64);
SELECT byteSwap(18439412204227788800::UInt64);
SELECT byteSwap(18446744073709551615::UInt64);

SELECT byteSwap(-0::Int8);
SELECT byteSwap(-1::Int8);
SELECT byteSwap(-128::Int8);

SELECT byteSwap(-129::Int16);
SELECT byteSwap(-4135::Int16);
SELECT byteSwap(-32768::Int16);

SELECT byteSwap(-32769::Int32);
SELECT byteSwap(-3351772109::Int32);
SELECT byteSwap(-2147483648::Int32);

SELECT byteSwap(-2147483649::Int64);
SELECT byteSwap(-1242525266376::Int64);
SELECT byteSwap(-9223372036854775808::Int64);

SELECT byteSwap(18446744073709551616::UInt128);
SELECT byteSwap(-9223372036854775809::Int128);

SELECT byteSwap(340282366920938463463374607431768211456::UInt256);
SELECT byteSwap(-170141183460469231731687303715884105729::Int256);

-- Booleans are interpreted as UInt8
SELECT byteSwap(false);
SELECT byteSwap(true);

-- Number of arguments should equal 1
SELECT byteSwap();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT byteSwap(128, 129);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Input should be integral
SELECT byteSwap('abc');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toFixedString('abc', 3));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toDate('2019-01-01'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toDate32('2019-01-01'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toDateTime32(1546300800));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toDateTime64(1546300800, 3));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(generateUUIDv4()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toDecimal32(2, 4));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT byteSwap(toFloat32(123.456));  -- { serverError NOT_IMPLEMENTED }
SELECT byteSwap(toFloat64(123.456));  -- { serverError NOT_IMPLEMENTED }

