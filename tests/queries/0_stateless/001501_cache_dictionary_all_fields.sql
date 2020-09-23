CREATE TABLE table_cache_dict(
KeyField UInt64,
UInt8_ UInt8,
UInt16_ UInt16,
UInt32_ UInt32,
UInt64_ UInt64,
Int8_ Int8,
Int16_ Int16,
Int32_ Int32,
Int64_ Int64,
UUID_ UUID,
Date_ Date,
DateTime_ DateTime,
String_ String,
Float32_ Float32,
Float64_ Float64,
ParentKeyField UInt64)
ENGINE = MergeTree() ORDER BY KeyField;


CREATE DICTIONARY IF NOT EXISTS cache_dict (
	KeyField UInt64 DEFAULT 9999999,
	UInt8_ UInt8 DEFAULT 55,
	UInt16_ UInt16 DEFAULT 65535,
	UInt32_ UInt32 DEFAULT 4294967295,
	UInt64_ UInt64 DEFAULT 18446744073709551615,
	Int8_ Int8 DEFAULT -128,
	Int16_ Int16 DEFAULT -32768,
	Int32_ Int32 DEFAULT -2147483648,
	Int64_ Int64 DEFAULT -9223372036854775808,
	UUID_ UUID DEFAULT '550e8400-0000-0000-0000-000000000000',
	Date_ Date DEFAULT '2018-12-30',
	DateTime_ DateTime DEFAULT '2018-12-30 00:00:00',
	String_ String DEFAULT 'hi',
	Float32_ Float32 DEFAULT 555.11,
	Float64_ Float64 DEFAULT 777.11,
	ParentKeyField UInt64 DEFAULT 444)
PRIMARY KEY KeyField 
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_cache_dict' DB 'default')) 
LIFETIME(2) LAYOUT(CACHE(SIZE_IN_CELLS 1));


INSERT INTO table_cache_dict VALUES (1, 22, 333, 4444, 55555, -6, -77, -888, -999, '550e8400-e29b-41d4-a716-446655440003', '1973-06-28', '1985-02-28 23:43:25', 'clickhouse', 22.543, 3332154213.4, 0);
INSERT INTO table_cache_dict VALUES (2, 3, 4, 5, 6, -7, -8, -9, -10, '550e8400-e29b-41d4-a716-446655440002', '1978-06-28', '1986-02-28 23:42:25', 'hello', 21.543, 3222154213.4, 1]);



SELECT arrayDistinct(groupArray(dictGetUInt8('default.cache_dict', 'UInt8_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt16('default.cache_dict', 'UInt16_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt32('default.cache_dict', 'UInt32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt64('default.cache_dict', 'UInt64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt8('default.cache_dict', 'Int8_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt16('default.cache_dict', 'Int16_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt32('default.cache_dict', 'Int32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt64('default.cache_dict', 'Int64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetFloat32('default.cache_dict', 'Float32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetFloat64('default.cache_dict', 'Float64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetString('default.cache_dict', 'String_', toUInt64(number)))) from numbers(10);