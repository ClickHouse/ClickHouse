/*DROP TABLE IF EXISTS cbor;
CREATE TABLE cbor (uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, int8 Int8, int16 Int16, int32 Int32, int64 Int64, float Float32, double Float64, string String, date Date) ENGINE = Memory;
INSERT INTO cbor VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'String', 18980), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', 20000),(42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', 42);
SELECT * FROM cbor FORMAT CBOR;
DROP TABLE cbor;

CREATE TABLE cbor (array1 Array(Array(UInt32)), array2 Array(Array(Array(String)))) ENGINE = Memory;
INSERT INTO cbor VALUES ([[1,2,3], [1001, 2002], [3167]], [[['one'], ['two']], [['three']],[['four'], ['five']]]);
SELECT * FROM cbor FORMAT CBOR;
DROP TABLE cbor;*/

CREATE TABLE cbor (bignums Array(UInt256)) ENGINE = Memory;
INSERT INTO cbor VALUES ([42, 1984, 1000000000000000, 3141592653589793238462643383]);
SELECT * FROM cbor FORMAT CBOR;
DROP TABLE cbor;
