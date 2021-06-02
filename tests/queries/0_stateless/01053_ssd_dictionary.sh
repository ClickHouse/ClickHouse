#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
  DROP DATABASE IF EXISTS 01053_db;

  CREATE DATABASE 01053_db Engine = Ordinary;

  DROP TABLE IF EXISTS 01053_db.table_for_dict;

  CREATE TABLE 01053_db.table_for_dict
  (
    id UInt64,
    a UInt64,
    b Int32,
    c String
  )
  ENGINE = MergeTree()
  ORDER BY id;

  INSERT INTO 01053_db.table_for_dict VALUES (1, 100, -100, 'clickhouse'), (2, 3, 4, 'database'), (5, 6, 7, 'columns'), (10, 9, 8, '');
  INSERT INTO 01053_db.table_for_dict SELECT number, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
  INSERT INTO 01053_db.table_for_dict SELECT number, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
  INSERT INTO 01053_db.table_for_dict SELECT number, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

  DROP DICTIONARY IF EXISTS 01053_db.ssd_dict;

  -- Probably we need rewrite it to integration test
  CREATE DICTIONARY 01053_db.ssd_dict
  (
      id UInt64,
      a UInt64 DEFAULT 0,
      b Int32 DEFAULT -1,
      c String DEFAULT 'none'
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01053_db'))
  LIFETIME(MIN 1000 MAX 2000)
  LAYOUT(SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

  SELECT 'TEST_SMALL';
  SELECT dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(1));
  SELECT dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(4));
  SELECT dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(5));
  SELECT dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(6));
  SELECT dictGetString('01053_db.ssd_dict', 'c', toUInt64(2));
  SELECT dictGetString('01053_db.ssd_dict', 'c', toUInt64(3));

  SELECT * FROM 01053_db.ssd_dict ORDER BY id;
  DROP DICTIONARY 01053_db.ssd_dict;

  DROP TABLE IF EXISTS 01053_db.keys_table;

  CREATE TABLE 01053_db.keys_table
  (
    id UInt64
  )
  ENGINE = StripeLog();

  INSERT INTO 01053_db.keys_table VALUES (1);
  INSERT INTO 01053_db.keys_table SELECT 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370;
  INSERT INTO 01053_db.keys_table VALUES (2);
  INSERT INTO 01053_db.keys_table SELECT 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370, 370;
  INSERT INTO 01053_db.keys_table VALUES (5);
  INSERT INTO 01053_db.keys_table SELECT 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 700, 370;
  INSERT INTO 01053_db.keys_table VALUES (10);

  DROP DICTIONARY IF EXISTS 01053_db.ssd_dict;

  CREATE DICTIONARY 01053_db.ssd_dict
  (
      id UInt64,
      a UInt64 DEFAULT 0,
      b Int32 DEFAULT -1,
      c String DEFAULT 'none'
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01053_db'))
  LIFETIME(MIN 1000 MAX 2000)
  LAYOUT(SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/1d' BLOCK_SIZE 512 WRITE_BUFFER_SIZE 4096));

  SELECT 'UPDATE DICTIONARY';
  SELECT sum(dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(id))) FROM 01053_db.keys_table;

  SELECT 'VALUE FROM DISK';
  SELECT dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(1));

  SELECT dictGetString('01053_db.ssd_dict', 'c', toUInt64(1));

  SELECT 'VALUE FROM RAM BUFFER';
  SELECT dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(10));
  SELECT dictGetString('01053_db.ssd_dict', 'c', toUInt64(10));

  SELECT 'VALUES FROM DISK AND RAM BUFFER';
  SELECT sum(dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(id))) FROM 01053_db.keys_table;

  SELECT 'HAS';
  SELECT count() FROM 01053_db.keys_table WHERE dictHas('01053_db.ssd_dict', toUInt64(id));

  SELECT 'VALUES NOT FROM TABLE';

  SELECT dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(1000000)), dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(1000000)), dictGetString('01053_db.ssd_dict', 'c', toUInt64(1000000));
  SELECT dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(1000000)), dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(1000000)), dictGetString('01053_db.ssd_dict', 'c', toUInt64(1000000));

  SELECT 'DUPLICATE KEYS';
  SELECT arrayJoin([1, 2, 3, 3, 2, 1]) AS id, dictGetInt32('01053_db.ssd_dict', 'b', toUInt64(id));
  --SELECT
  DROP DICTIONARY IF EXISTS 01053_db.ssd_dict;

  DROP TABLE IF EXISTS 01053_db.keys_table;

  CREATE TABLE 01053_db.keys_table
  (
    id UInt64
  )
  ENGINE = MergeTree()
  ORDER BY id;

  INSERT INTO 01053_db.keys_table VALUES (1);
  INSERT INTO 01053_db.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370;
  INSERT INTO 01053_db.keys_table VALUES (2);
  INSERT INTO 01053_db.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 370, 370;
  INSERT INTO 01053_db.keys_table VALUES (5);
  INSERT INTO 01053_db.keys_table SELECT intHash64(number) FROM system.numbers LIMIT 700, 370;
  INSERT INTO 01053_db.keys_table VALUES (10);

  OPTIMIZE TABLE 01053_db.keys_table;

  CREATE DICTIONARY 01053_db.ssd_dict
  (
      id UInt64,
      a UInt64 DEFAULT 0,
      b Int32 DEFAULT -1
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01053_db'))
  LIFETIME(MIN 1000 MAX 2000)
  LAYOUT(SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/2d' BLOCK_SIZE 512 WRITE_BUFFER_SIZE 1024));

  SELECT 'UPDATE DICTIONARY (MT)';
  SELECT sum(dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(id))) FROM 01053_db.keys_table;

  SELECT 'VALUES FROM DISK AND RAM BUFFER (MT)';
  SELECT sum(dictGetUInt64('01053_db.ssd_dict', 'a', toUInt64(id))) FROM 01053_db.keys_table;

  DROP DICTIONARY IF EXISTS 01053_db.ssd_dict;

  DROP TABLE IF EXISTS 01053_db.table_for_dict;

  DROP DATABASE IF EXISTS 01053_db;"
