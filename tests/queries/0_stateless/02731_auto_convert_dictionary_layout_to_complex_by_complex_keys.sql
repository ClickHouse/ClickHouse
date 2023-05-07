DROP DICTIONARY IF EXISTS dict_flat_simple;
DROP DICTIONARY IF EXISTS dict_hashed_simple;
DROP DICTIONARY IF EXISTS dict_hashed_complex;
DROP DICTIONARY IF EXISTS dict_hashed_simple_auto_convert;
DROP TABLE IF EXISTS dict_data;

CREATE TABLE dict_data (v0 UInt16, v1 UInt16, v2 UInt16, v3 UInt16, v4 UInt16) engine=Memory();

CREATE DICTIONARY dict_flat_simple (v0 UInt16, v1 UInt16, v2 UInt16) PRIMARY KEY v0 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(flat());
SYSTEM RELOAD DICTIONARY dict_flat_simple;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_flat_simple';
DROP DICTIONARY dict_flat_simple;

CREATE DICTIONARY dict_hashed_simple (v0 UInt16, v1 UInt16, v2 UInt16) PRIMARY KEY v0 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple';
DROP DICTIONARY dict_hashed_simple;

CREATE DICTIONARY dict_hashed_complex (v0 UInt16, v1 UInt16, v2 UInt16) PRIMARY KEY v0,v1 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(complex_key_hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_complex;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_complex';
DROP DICTIONARY dict_hashed_complex;

CREATE DICTIONARY dict_hashed_simple_auto_convert (v0 UInt16, v1 UInt16, v2 UInt16) PRIMARY KEY v0,v1 SOURCE(CLICKHOUSE(TABLE 'dict_data')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_auto_convert;
SELECT name, type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_hashed_simple_auto_convert';
DROP DICTIONARY dict_hashed_simple_auto_convert;

DROP TABLE dict_data;
