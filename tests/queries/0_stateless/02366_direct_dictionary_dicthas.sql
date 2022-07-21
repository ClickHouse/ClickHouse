-- Tags: no-backward-compatibility-check
DROP DATABASE IF EXISTS 02366_dictionary_db;
CREATE DATABASE 02366_dictionary_db;

CREATE TABLE 02366_dictionary_db.dict_data
(
   id UInt64,
   val String
)
ENGINE = Memory;

CREATE TABLE 02366_dictionary_db.lookup_data
(
   id UInt64,
   lookup_key UInt64,
)
ENGINE = Memory;

INSERT INTO 02366_dictionary_db.dict_data VALUES(0, 'value_0');

INSERT INTO 02366_dictionary_db.lookup_data VALUES(0, 0);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(1, 0);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(2, 0);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(3, 1);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(4, 0);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(5, 1);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(6, 0);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(7, 2);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(8, 1);
INSERT INTO 02366_dictionary_db.lookup_data VALUES(9, 0);

CREATE DICTIONARY 02366_dictionary_db.dict0
(
   id UInt64,
   val String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data'))
LAYOUT(DIRECT());

SELECT lookup_key FROM 02366_dictionary_db.lookup_data ORDER BY id ASC;
SELECT id, lookup_key, dictHas(02366_dictionary_db.dict0, lookup_key) FROM 02366_dictionary_db.lookup_data ORDER BY id ASC;

-- Nesting this way seems to help it make all the lookups as a single block, although even then it isn't guaranteed
SELECT dictHas(02366_dictionary_db.dict0, lk) FROM (SELECT any(lookup_key) as lk FROM 02366_dictionary_db.lookup_data group by id ORDER BY id ASC);
-- Same with this group by
SELECT dictHas(02366_dictionary_db.dict0, any(lookup_key)) FROM 02366_dictionary_db.lookup_data GROUP BY id ORDER BY id ASC;


SELECT dictHas(02366_dictionary_db.dict0, lookup_key) FROM 02366_dictionary_db.lookup_data ORDER BY id ASC;
SELECT dictGetOrDefault(02366_dictionary_db.dict0, 'val', lookup_key, 'UNKNOWN') FROM 02366_dictionary_db.lookup_data ORDER BY id ASC;
SELECT count(), has FROM 02366_dictionary_db.lookup_data group by dictHas(02366_dictionary_db.dict0, lookup_key) as has;

DROP DICTIONARY 02366_dictionary_db.dict0;
DROP TABLE 02366_dictionary_db.lookup_data;
DROP TABLE 02366_dictionary_db.dict_data;
