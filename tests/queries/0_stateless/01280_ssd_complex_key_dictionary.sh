#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP DATABASE IF EXISTS 01280_db;
    CREATE DATABASE 01280_db Engine = Ordinary;
    DROP TABLE IF EXISTS 01280_db.table_for_dict;
    CREATE TABLE 01280_db.table_for_dict
    (
        k1 String,
        k2 Int32,
        a UInt64,
        b Int32,
        c String
    )
    ENGINE = MergeTree()
    ORDER BY (k1, k2);

    INSERT INTO 01280_db.table_for_dict VALUES (toString(1), 3, 100, -100, 'clickhouse'), (toString(2), -1, 3, 4, 'database'), (toString(5), -3, 6, 7, 'columns'), (toString(10), -20, 9, 8, '');
    INSERT INTO 01280_db.table_for_dict SELECT toString(number), number + 1, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
    INSERT INTO 01280_db.table_for_dict SELECT toString(number), number + 10, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
    INSERT INTO 01280_db.table_for_dict SELECT toString(number), number + 100, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

    DROP DICTIONARY IF EXISTS 01280_db.ssd_dict;
    CREATE DICTIONARY 01280_db.ssd_dict
    (
        k1 String,
        k2 Int32,
        a UInt64 DEFAULT 0,
        b Int32 DEFAULT -1,
        c String DEFAULT 'none'
    )
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01280_db'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));"

$CLICKHOUSE_CLIENT -nq "SELECT dictHas('01280_db.ssd_dict', 'a', tuple('1')); -- { serverError 43 }"

$CLICKHOUSE_CLIENT -n --query="
    SELECT 'TEST_SMALL';
    SELECT 'VALUE FROM RAM BUFFER';
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('1', toInt32(3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('1', toInt32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('1', toInt32(3)));
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('1', toInt32(3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('1', toInt32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('1', toInt32(3)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('2', toInt32(-1)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('2', toInt32(-1)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('2', toInt32(-1)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('5', toInt32(-3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('5', toInt32(-3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('5', toInt32(-3)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('10', toInt32(-20)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('10', toInt32(-20)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('10', toInt32(-20)));"

$CLICKHOUSE_CLIENT -nq "SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple(toInt32(3))); -- { serverError 53 }"

$CLICKHOUSE_CLIENT -n --query="DROP DICTIONARY 01280_db.ssd_dict;
    DROP TABLE IF EXISTS 01280_db.keys_table;
    CREATE TABLE 01280_db.keys_table
    (
        k1 String,
        k2 Int32
    )
    ENGINE = StripeLog();

    INSERT INTO 01280_db.keys_table VALUES ('1', 3);
    INSERT INTO 01280_db.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370;
    INSERT INTO 01280_db.keys_table VALUES ('2', -1);
    INSERT INTO 01280_db.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370, 370;
    INSERT INTO 01280_db.keys_table VALUES ('5', -3);
    INSERT INTO 01280_db.keys_table SELECT toString(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 700, 370;
    INSERT INTO 01280_db.keys_table VALUES ('10', -20);

    DROP DICTIONARY IF EXISTS 01280_db.ssd_dict;CREATE DICTIONARY 01280_db.ssd_dict
    (
        k1 String,
        k2 Int32,
        a UInt64 DEFAULT 0,
        b Int32 DEFAULT -1,
        c String DEFAULT 'none'
    )
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01280_db'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/1d' BLOCK_SIZE 512 WRITE_BUFFER_SIZE 4096));

    SELECT 'UPDATE DICTIONARY';

    SELECT sum(dictGetUInt64('01280_db.ssd_dict', 'a', (k1, k2))) FROM 01280_db.keys_table;

    SELECT 'VALUE FROM DISK';
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', ('1', toInt32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', ('1', toInt32(3)));

    SELECT 'VALUE FROM RAM BUFFER';
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', ('10', toInt32(-20)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', ('10', toInt32(-20)));

    SELECT 'VALUES FROM DISK AND RAM BUFFER';
    SELECT sum(dictGetUInt64('01280_db.ssd_dict', 'a', (k1, k2))) FROM 01280_db.keys_table;

    SELECT 'HAS';
    SELECT count() FROM 01280_db.keys_table WHERE dictHas('01280_db.ssd_dict', (k1, k2));

    SELECT 'VALUES NOT FROM TABLE';
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', ('unknown', toInt32(0))), dictGetInt32('01280_db.ssd_dict', 'b', ('unknown', toInt32(0))), dictGetString('01280_db.ssd_dict', 'c', ('unknown', toInt32(0)));
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', ('unknown', toInt32(0))), dictGetInt32('01280_db.ssd_dict', 'b', ('unknown', toInt32(0))), dictGetString('01280_db.ssd_dict', 'c', ('unknown', toInt32(0)));

    SELECT 'DUPLICATE KEYS';
    SELECT arrayJoin([('1', toInt32(3)), ('2', toInt32(-1)), ('', toInt32(0)), ('', toInt32(0)), ('2', toInt32(-1)), ('1', toInt32(3))]) AS keys, dictGetInt32('01280_db.ssd_dict', 'b', keys);
    DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;
    DROP TABLE IF EXISTS database_for_dict.keys_table;"

$CLICKHOUSE_CLIENT -n --query="DROP DATABASE IF EXISTS 01280_db;"
