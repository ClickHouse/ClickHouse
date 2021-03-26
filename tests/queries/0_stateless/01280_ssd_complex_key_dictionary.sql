SET send_logs_level = 'none';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict Engine = Ordinary;

DROP TABLE IF EXISTS database_for_dict.table_for_dict;

CREATE TABLE database_for_dict.table_for_dict
(
  k1 String,
  k2 Int32,
  a UInt64,
  b Int32,
  c String
)
ENGINE = MergeTree()
ORDER BY (k1, k2);

INSERT INTO database_for_dict.table_for_dict VALUES (toString(1), 3, 100, -100, 'clickhouse'), (toString(2), -1, 3, 4, 'database'), (toString(5), -3, 6, 7, 'columns'), (toString(10), -20, 9, 8, '');
INSERT INTO database_for_dict.table_for_dict SELECT toString(number), number + 1, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
INSERT INTO database_for_dict.table_for_dict SELECT toString(number), number + 10, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
INSERT INTO database_for_dict.table_for_dict SELECT toString(number), number + 100, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

CREATE DICTIONARY database_for_dict.ssd_dict
(
    k1 String,
    k2 Int32,
    a UInt64 DEFAULT 0,
    b Int32 DEFAULT -1,
    c String DEFAULT 'none'
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1000 MAX 2000)
LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '/var/lib/clickhouse/clickhouse_dicts/0d'));

SELECT 'TEST_SMALL';
SELECT 'VALUE FROM RAM BUFFER';

SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', tuple('1', toInt32(3)));
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', tuple('1', toInt32(3)));
SELECT dictGetString('database_for_dict.ssd_dict', 'c', tuple('1', toInt32(3)));

SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', tuple('1', toInt32(3)));
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', tuple('1', toInt32(3)));
SELECT dictGetString('database_for_dict.ssd_dict', 'c', tuple('1', toInt32(3)));

SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', tuple('2', toInt32(-1)));
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', tuple('2', toInt32(-1)));
SELECT dictGetString('database_for_dict.ssd_dict', 'c', tuple('2', toInt32(-1)));

SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', tuple('5', toInt32(-3)));
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', tuple('5', toInt32(-3)));
SELECT dictGetString('database_for_dict.ssd_dict', 'c', tuple('5', toInt32(-3)));

SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', tuple('10', toInt32(-20)));
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', tuple('10', toInt32(-20)));
SELECT dictGetString('database_for_dict.ssd_dict', 'c', tuple('10', toInt32(-20)));

DROP DICTIONARY database_for_dict.ssd_dict;

DROP TABLE IF EXISTS database_for_dict.keys_table;

CREATE TABLE database_for_dict.keys_table
(
    k1 String,
    k2 Int32
)
ENGINE = StripeLog();

INSERT INTO database_for_dict.keys_table VALUES ('1', 3);
INSERT INTO database_for_dict.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370;
INSERT INTO database_for_dict.keys_table VALUES ('2', -1);
INSERT INTO database_for_dict.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370, 370;
INSERT INTO database_for_dict.keys_table VALUES ('5', -3);
INSERT INTO database_for_dict.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 700, 370;
INSERT INTO database_for_dict.keys_table VALUES ('10', -20);

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

CREATE DICTIONARY database_for_dict.ssd_dict
(
    k1 String,
    k2 Int32,
    a UInt64 DEFAULT 0,
    b Int32 DEFAULT -1,
    c String DEFAULT 'none'
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1000 MAX 2000)
LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '/var/lib/clickhouse/clickhouse_dicts/1d' BLOCK_SIZE 512 WRITE_BUFFER_SIZE 4096 MAX_STORED_KEYS 1000000));

SELECT 'UPDATE DICTIONARY';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', (k1, k2))) FROM database_for_dict.keys_table;

SELECT 'VALUE FROM DISK';
-- -100
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', ('1', toInt32(3)));

-- 'clickhouse'
SELECT dictGetString('database_for_dict.ssd_dict', 'c', ('1', toInt32(3)));

SELECT 'VALUE FROM RAM BUFFER';
-- 8
SELECT dictGetInt32('database_for_dict.ssd_dict', 'b', ('10', toInt32(-20)));

-- ''
SELECT dictGetString('database_for_dict.ssd_dict', 'c', ('10', toInt32(-20)));

SELECT 'VALUES FROM DISK AND RAM BUFFER';
-- 118
SELECT sum(dictGetUInt64('database_for_dict.ssd_dict', 'a', (k1, k2))) FROM database_for_dict.keys_table;

SELECT 'HAS';
-- 6
SELECT count() FROM database_for_dict.keys_table WHERE dictHas('database_for_dict.ssd_dict', (k1, k2));

SELECT 'VALUES NOT FROM TABLE';
-- 0 -1 none
SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', ('unknown', toInt32(0))), dictGetInt32('database_for_dict.ssd_dict', 'b', ('unknown', toInt32(0))), dictGetString('database_for_dict.ssd_dict', 'c', ('unknown', toInt32(0)));
SELECT dictGetUInt64('database_for_dict.ssd_dict', 'a', ('unknown', toInt32(0))), dictGetInt32('database_for_dict.ssd_dict', 'b', ('unknown', toInt32(0))), dictGetString('database_for_dict.ssd_dict', 'c', ('unknown', toInt32(0)));

SELECT 'DUPLICATE KEYS';
SELECT arrayJoin([('1', toInt32(3)), ('2', toInt32(-1)), ('', toInt32(0)), ('', toInt32(0)), ('2', toInt32(-1)), ('1', toInt32(3))]) AS keys, dictGetInt32('database_for_dict.ssd_dict', 'b', keys);

DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;

DROP TABLE IF EXISTS database_for_dict.keys_table;
