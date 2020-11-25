drop database if exists db_01501;
drop table if exists db_01501.table_cache_dict;
drop dictionary if exists db_01501.cache_dict;


create database db_01501;

CREATE TABLE db_01501.table_cache_dict(
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
Decimal32_ Decimal32(5),
Decimal64_ Decimal64(15),
Decimal128_ Decimal128(35),
ParentKeyField UInt64)
ENGINE = MergeTree() ORDER BY KeyField;


CREATE DICTIONARY IF NOT EXISTS db_01501.cache_dict (
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
	Float32_ Float32 DEFAULT 111.11,
	Float64_ Float64 DEFAULT 222.11,
	Decimal32_ Decimal32(5) DEFAULT 333.11,
	Decimal64_ Decimal64(15) DEFAULT 444.11,
	Decimal128_ Decimal128(35) DEFAULT 555.11,
	ParentKeyField UInt64 DEFAULT 444)
PRIMARY KEY KeyField 
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_cache_dict' DB 'db_01501')) 
LIFETIME(5) LAYOUT(CACHE(SIZE_IN_CELLS 20));


INSERT INTO db_01501.table_cache_dict VALUES (1, 2, 3, 4, 5, -1, -2, -3, -4, '550e8400-e29b-41d4-a716-446655440003', '1973-06-28', '1985-02-28 23:43:25', 'clickhouse', 22.543, 3332154213.4, toDecimal32('1e-5', 5), toDecimal64('1e-15', 15), toDecimal128('1e-35', 35), 0);
INSERT INTO db_01501.table_cache_dict VALUES (2, 22, 33, 44, 55, -11, -22, -33, -44, 'cb307805-44f0-49e7-9ae9-9954c543be46', '1978-06-28', '1986-02-28 23:42:25', 'hello', 21.543, 3111154213.9, toDecimal32('2e-5', 5), toDecimal64('2e-15', 15), toDecimal128('2e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (3, 222, 333, 444, 555, -111, -222, -333, -444, 'de7f7ec3-f851-4f8c-afe5-c977cb8cea8d', '1982-06-28', '1999-02-28 23:42:25', 'dbms', 13.334, 3222187213.1, toDecimal32('3e-5', 5), toDecimal64('3e-15', 15), toDecimal128('3e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (4, 2222, 3333, 4444, 5555, -1111, -2222, -3333, -4444, '4bd3829f-0669-43b7-b884-a8e034a68224', '1987-06-28', '2000-02-28 23:42:25', 'MergeTree', 52.001, 3237554213.5, toDecimal32('4e-5', 5), toDecimal64('4e-15', 15), toDecimal128('4e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (5, 22222, 33333, 44444, 55555, -11111, -22222, -33333, -44444, 'ff99a408-78bb-4939-93cc-65e657e347c6', '1991-06-28', '2007-02-28 23:42:25', 'dictionary', 33.333, 3222193713.7, toDecimal32('5e-5', 5), toDecimal64('5e-15', 15), toDecimal128('5e-35', 35), 1);


SELECT arrayDistinct(groupArray(dictGetUInt8('db_01501.cache_dict', 'UInt8_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetUInt16('db_01501.cache_dict', 'UInt16_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetUInt32('db_01501.cache_dict', 'UInt32_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetUInt64('db_01501.cache_dict', 'UInt64_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetInt8('db_01501.cache_dict', 'Int8_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetInt16('db_01501.cache_dict', 'Int16_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetInt32('db_01501.cache_dict', 'Int32_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetInt64('db_01501.cache_dict', 'Int64_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetFloat32('db_01501.cache_dict', 'Float32_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetFloat64('db_01501.cache_dict', 'Float64_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal32_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal64_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal128_', toUInt64(number)))) from numbers(10);
system reload dictionaries; 
SELECT arrayDistinct(groupArray(dictGetString('db_01501.cache_dict', 'String_', toUInt64(number)))) from numbers(10);



SELECT arrayDistinct(groupArray(dictGetUInt8('db_01501.cache_dict', 'UInt8_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt16('db_01501.cache_dict', 'UInt16_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt32('db_01501.cache_dict', 'UInt32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetUInt64('db_01501.cache_dict', 'UInt64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt8('db_01501.cache_dict', 'Int8_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt16('db_01501.cache_dict', 'Int16_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt32('db_01501.cache_dict', 'Int32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetInt64('db_01501.cache_dict', 'Int64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetFloat32('db_01501.cache_dict', 'Float32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetFloat64('db_01501.cache_dict', 'Float64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal32_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal64_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGet('db_01501.cache_dict', 'Decimal128_', toUInt64(number)))) from numbers(10);
SELECT arrayDistinct(groupArray(dictGetString('db_01501.cache_dict', 'String_', toUInt64(number)))) from numbers(10);


system reload dictionaries; 


SELECT groupArray(dictHas('db_01501.cache_dict', toUInt64(number))) from numbers(10);
SELECT groupArray(dictHas('db_01501.cache_dict', toUInt64(number))) from numbers(10);
SELECT groupArray(dictHas('db_01501.cache_dict', toUInt64(number))) from numbers(10);
SELECT groupArray(dictHas('db_01501.cache_dict', toUInt64(number))) from numbers(10);
SELECT groupArray(dictHas('db_01501.cache_dict', toUInt64(number))) from numbers(10);

drop table if exists table_cache_dict;
drop dictionary if exists cache_dict;
drop database if exists db_01501;