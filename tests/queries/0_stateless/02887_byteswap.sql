/* UInt8 */
SELECT byteSwap(0);
SELECT byteSwap(1);
SELECT byteSwap(255);

/* UInt16 */
SELECT byteSwap(256);
SELECT byteSwap(4135);
SELECT byteSwap(10000);
SELECT byteSwap(65535);

/* UInt32 */
SELECT byteSwap(65536);
SELECT byteSwap(3351772109);
SELECT byteSwap(3455829959);
SELECT byteSwap(4294967295);

/* UInt64 */
SELECT byteSwap(4294967296);
SELECT byteSwap(123294967295);
SELECT byteSwap(18439412204227788800);
SELECT byteSwap(18446744073709551615);

/* Int8 */
SELECT byteSwap(-0);
SELECT byteSwap(-1);
SELECT byteSwap(-128);

/* Int16 */
SELECT byteSwap(-129);
SELECT byteSwap(-4135);
SELECT byteSwap(-32768);

/* Int32 */
SELECT byteSwap(-32769);
SELECT byteSwap(-3351772109);
SELECT byteSwap(-2147483648);

/* Int64 */
SELECT byteSwap(-2147483649);
SELECT byteSwap(-1242525266376);
SELECT byteSwap(-9223372036854775808);

/* Booleans are interpreted as UInt8 */
SELECT byteSwap(false);
SELECT byteSwap(true);

/* Integer overflows */
SELECT byteSwap(18446744073709551616);  -- { serverError 48 }
SELECT byteSwap(-9223372036854775809);  -- { serverError 48 }

/* Number of arguments should equal 1 */
SELECT byteSwap();  -- { serverError 42 }
SELECT byteSwap(128, 129);  -- { serverError 42 }

/* Input should be "integral" */
SELECT byteSwap('abc');  -- { serverError 43 }
SELECT byteSwap(reinterpretAsFixedString(3351772109));  -- { serverError 43 }
SELECT byteSwap(toDate('2019-01-01'));  -- { serverError 43 }
SELECT byteSwap(toDate32('2019-01-01'));  -- { serverError 43 }
SELECT byteSwap(toDateTime32(1546300800));  -- { serverError 43 }
SELECT byteSwap(toDateTime64(1546300800, 3));  -- { serverError 43 }
SELECT byteSwap(generateUUIDv4()); -- { serverError 43 }
SELECT byteSwap(toDecimal32(2, 4));  -- { serverError 43 }
SELECT byteSwap(toFloat32(123.456));  -- { serverError 48 }
SELECT byteSwap(toFloat64(123.456));  -- { serverError 48 }
