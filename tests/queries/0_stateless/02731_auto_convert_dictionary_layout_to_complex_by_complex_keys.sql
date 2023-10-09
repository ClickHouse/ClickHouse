DROP DICTIONARY IF EXISTS dict_flat_simple;
DROP DICTIONARY IF EXISTS dict_hashed_simple_Decimal128;
DROP DICTIONARY IF EXISTS dict_hashed_simple_Float32;
DROP DICTIONARY IF EXISTS dict_hashed_simple_String;
DROP DICTIONARY IF EXISTS dict_hashed_simple_auto_convert;
DROP TABLE IF EXISTS dict_data;

CREATE TABLE dict_data (v0 UInt16, v1 Int16, v2 Float32, v3 Decimal128(10), v4 String) engine=Memory()  AS SELECT number, number%65535, number*1.1, number*1.1, 'foo' FROM numbers(10);;

CREATE DICTIONARY dict_flat_simple (v0 UInt16, v1 UInt16, v2 UInt16) PRIMARY KEY v0 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(flat());
SYSTEM RELOAD DICTIONARY dict_flat_simple;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_flat_simple';
DROP DICTIONARY dict_flat_simple;

CREATE DICTIONARY dict_hashed_simple_Decimal128 (v3 Decimal128(10), v1 UInt16, v2 Float32) PRIMARY KEY v3 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_Decimal128;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple_Decimal128';
DROP DICTIONARY dict_hashed_simple_Decimal128;

CREATE DICTIONARY dict_hashed_simple_Float32 (v2 Float32, v3 Decimal128(10), v4 String) PRIMARY KEY v2 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_Float32;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple_Float32';
DROP DICTIONARY dict_hashed_simple_Float32;

CREATE DICTIONARY dict_hashed_simple_String (v4 String, v3 Decimal128(10), v2 Float32) PRIMARY KEY v4 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_String;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple_String';
DROP DICTIONARY dict_hashed_simple_String;

CREATE DICTIONARY dict_hashed_simple_auto_convert (v0 UInt16, v1 Int16, v2 UInt16) PRIMARY KEY v0,v1 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_auto_convert;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple_auto_convert';
DROP DICTIONARY dict_hashed_simple_auto_convert;

DROP TABLE dict_data;
