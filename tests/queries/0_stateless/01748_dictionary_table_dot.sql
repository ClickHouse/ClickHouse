DROP DATABASE IF EXISTS test_01748;
CREATE DATABASE test_01748;
USE test_01748;

DROP TABLE IF EXISTS `test.txt`;
DROP DICTIONARY IF EXISTS test_dict;

CREATE TABLE `test.txt`
(
    `key1` UInt32,
    `key2` UInt32,
    `value` String
)
ENGINE = Memory();

CREATE DICTIONARY test_dict
(
    `key1` UInt32,
    `key2` UInt32,
    `value` String
)
PRIMARY KEY key1, key2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE `test.txt` PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 1 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

INSERT INTO `test.txt` VALUES (1, 2, 'Hello');

-- TODO: it does not work without fully qualified name.
SYSTEM RELOAD DICTIONARY test_01748.test_dict;

SELECT dictGet(test_dict, 'value', (toUInt32(1), toUInt32(2)));

DROP DATABASE test_01748;
