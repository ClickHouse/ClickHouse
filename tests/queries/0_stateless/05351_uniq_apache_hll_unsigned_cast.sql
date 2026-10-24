-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- An unsigned `UInt8`, `UInt16` or `UInt32` value is hashed as the same number as a 64-bit integer. The C++ library hashes such a value
-- through the signed type of the same width, and its sketch is what the documented casts to that type give.

SELECT 'by value, as Java and Spark hash a long';
SELECT hex(toString(uniqApacheHLLState(toUInt32(3000000000)))) = hex(toString(uniqApacheHLLState(toInt64(3000000000))));
SELECT hex(toString(uniqApacheHLLState(toUInt16(50000)))) = hex(toString(uniqApacheHLLState(toInt64(50000))));
SELECT hex(toString(uniqApacheHLLState(toUInt8(200)))) = hex(toString(uniqApacheHLLState(toInt64(200))));

SELECT 'a cast to the signed type of the same width gives the sketch of the C++ library';
SELECT hex(toString(uniqApacheHLLState(toInt32(toUInt32(3000000000))))) = hex(toString(uniqApacheHLLState(toInt64(-1294967296))));
SELECT hex(toString(uniqApacheHLLState(toInt16(toUInt16(50000))))) = hex(toString(uniqApacheHLLState(toInt64(-15536))));
SELECT hex(toString(uniqApacheHLLState(toInt8(toUInt8(200))))) = hex(toString(uniqApacheHLLState(toInt64(-56))));
SELECT hex(toString(uniqApacheHLLState(toInt32(toUInt32(toIPv4('192.168.0.1')))))) = hex(toString(uniqApacheHLLState(toInt64(-1062731775))));
SELECT hex(toString(uniqApacheHLLState(toInt32(toDateTime(2147483648, 'UTC'))))) = hex(toString(uniqApacheHLLState(toInt64(-2147483648))));
SELECT hex(toString(uniqApacheHLLState(toInt16(toUInt16(toDate(32768)))))) = hex(toString(uniqApacheHLLState(toInt64(-32768))));

SELECT 'the two are different sketches for these values';
SELECT hex(toString(uniqApacheHLLState(toUInt32(3000000000)))) != hex(toString(uniqApacheHLLState(toInt32(toUInt32(3000000000)))));

SELECT 'the values below the signed range are not affected';
SELECT hex(toString(uniqApacheHLLState(toUInt32(2147483647)))) = hex(toString(uniqApacheHLLState(toInt32(2147483647))));
SELECT hex(toString(uniqApacheHLLState(toIPv4('10.0.0.1')))) = hex(toString(uniqApacheHLLState(toInt32(toUInt32(toIPv4('10.0.0.1'))))));

SELECT 'a cast that checks the range rejects these values';
SELECT accurateCast(toUInt32(3000000000), 'Int32'); -- { serverError CANNOT_CONVERT_TYPE }
