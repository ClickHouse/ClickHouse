DROP DATABASE IF EXISTS dict_db_01224;
DROP DATABASE IF EXISTS dict_db_01224_dictionary;
CREATE DATABASE dict_db_01224;
CREATE DATABASE dict_db_01224_dictionary Engine=Dictionary;

CREATE TABLE dict_db_01224.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict_db_01224.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_01224'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT * FROM system.tables FORMAT Null;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SHOW CREATE TABLE dict_db_01224.dict FORMAT TSVRaw;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SHOW CREATE TABLE dict_db_01224_dictionary.`dict_db_01224.dict` FORMAT TSVRaw;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT engine, metadata_path LIKE '%/metadata/dict\_db\_01224/dict.sql', create_table_query FROM system.tables WHERE database = 'dict_db_01224' AND name = 'dict';
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT name, type FROM system.columns WHERE database = 'dict_db_01224' AND table = 'dict';
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

DROP DICTIONARY dict_db_01224.dict;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

DROP DATABASE dict_db_01224;
DROP DATABASE dict_db_01224_dictionary;
