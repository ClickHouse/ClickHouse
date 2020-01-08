SET send_logs_level = 'none';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict Engine = Ordinary;

DROP TABLE IF EXISTS database_for_dict.table_for_dict;

CREATE TABLE database_for_dict.table_for_dict
(
  id UInt64,
  a UInt64,
  b Int32
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO database_for_dict.table_for_dict VALUES (1, 100, -100), (2, 3, 4), (5, 6, 7), (10, 9, 8);

DROP TABLE IF EXISTS database_for_dict.keys_table;

CREATE TABLE database_for_dict.keys_table
(
  id UInt64
)
ENGINE = StripeLog();

INSERT INTO database_for_dict.keys_table VALUES (1);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370;
INSERT INTO database_for_dict.keys_table VALUES (2);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370, 370;
INSERT INTO database_for_dict.keys_table VALUES (5);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 700, 370;
INSERT INTO database_for_dict.keys_table VALUES (10);

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

CREATE DICTIONARY database_for_dict.ssd_dict
(
    id UInt64,
    a UInt64 DEFAULT 0,
    b Int32 DEFAULT -1
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1000 MAX 2000)
LAYOUT(SSD(MAX_PARTITION_SIZE 1000 PATH '/mnt/disk4/clickhouse_dicts/1'));

SELECT 'UPDATE DICTIONARY';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', toUInt64(id))) FROM database_for_dict.keys_table;

SELECT 'VALUE FROM DISK';
-- -100
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', toUInt64(1));

SELECT 'VALUE FROM RAM BUFFER';
-- 8
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', toUInt64(10));

SELECT 'VALUES FROM DISK AND RAM BUFFER';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', toUInt64(id))) FROM database_for_dict.keys_table;

SELECT 'VALUES NOT FROM TABLE';
-- 0 -1
SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', toUInt64(1000000)), dictGetInt32('database_for_dict.ssd_dict', 'b', toUInt64(1000000));

SELECT 'DUPLICATE KEYS';
SELECT arrayJoin([1, 2, 3, 3, 2, 1]) AS id, dictGetInt32('database_for_dict.ssd_dict', 'b', toUInt64(id));

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

DROP TABLE IF EXISTS database_for_dict.keys_table;

CREATE TABLE database_for_dict.keys_table
(
  id UInt64
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO database_for_dict.keys_table VALUES (1);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370;
INSERT INTO database_for_dict.keys_table VALUES (2);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370, 370;
INSERT INTO database_for_dict.keys_table VALUES (5);
INSERT INTO database_for_dict.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 700, 370;
INSERT INTO database_for_dict.keys_table VALUES (10);

-- one block
OPTIMIZE TABLE database_for_dict.keys_table;

CREATE DICTIONARY database_for_dict.ssd_dict
(
    id UInt64,
    a UInt64 DEFAULT 0,
    b Int32 DEFAULT -1
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1000 MAX 2000)
LAYOUT(SSD(MAX_PARTITION_SIZE 1000 PATH '/mnt/disk4/clickhouse_dicts/1'));

SELECT 'UPDATE DICTIONARY (MT)';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', toUInt64(id))) FROM database_for_dict.keys_table;

SELECT 'VALUES FROM DISK AND RAM BUFFER (MT)';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', toUInt64(id))) FROM database_for_dict.keys_table;

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

DROP TABLE IF EXISTS database_for_dict.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict;
