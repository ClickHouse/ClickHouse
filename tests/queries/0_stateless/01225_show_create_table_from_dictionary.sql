-- Tags: no-parallel

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS dict_db_01225;
DROP DATABASE IF EXISTS dict_db_01225_dictionary;
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE dict_db_01225 ENGINE=Ordinary;    -- Different internal dictionary name with Atomic
CREATE DATABASE dict_db_01225_dictionary Engine=Dictionary;

CREATE TABLE dict_db_01225.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict_db_01225.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_01225'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SHOW CREATE TABLE dict_db_01225_dictionary.`dict_db_01225.dict` FORMAT TSVRaw;
SHOW CREATE TABLE dict_db_01225_dictionary.`dict_db_01225.no_such_dict`; -- { serverError CANNOT_GET_CREATE_DICTIONARY_QUERY }

DROP DATABASE dict_db_01225;
DROP DATABASE dict_db_01225_dictionary;
