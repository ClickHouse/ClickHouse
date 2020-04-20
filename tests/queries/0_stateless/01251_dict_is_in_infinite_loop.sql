DROP TABLE IF EXISTS dict_source;
CREATE TABLE dict_source (id UInt64, parent_id UInt64, value String) ENGINE = Memory;
INSERT INTO dict_source VALUES (1, 0, 'hello'), (2, 1, 'world'), (3, 2, 'upyachka'), (11, 22, 'a'), (22, 11, 'b');

DROP DATABASE IF EXISTS database_for_dict;
CREATE DATABASE database_for_dict Engine = Ordinary;

DROP DICTIONARY IF EXISTS database_for_dict.dictionary_with_hierarchy;

CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    id UInt64, parent_id UInt64 HIERARCHICAL, value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' table 'dict_source'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1);

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(2), toUInt64(1));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(22), toUInt64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(toUInt64(22)), toUInt64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(11), materialize(toUInt64(22)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(toUInt64(22)), materialize(toUInt64(11)));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(22), toUInt64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(toUInt64(22)), toUInt64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(11), materialize(toUInt64(222)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(toUInt64(22)), materialize(toUInt64(111)));

DROP DICTIONARY database_for_dict.dictionary_with_hierarchy;
DROP TABLE dict_source;
DROP DATABASE database_for_dict;
