#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# ========== Test Clickhouse Local ==========
$CLICKHOUSE_LOCAL --query "SELECT '==========LOCAL===========';"

$CLICKHOUSE_LOCAL --query "
    SELECT 'Normal';
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(Float32, 7)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7]);
    INSERT INTO qbit VALUES (2, [9, 10, 11, 12, 13, 14, 15]);
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"

# RowBinary on local ClickHouse: [de]serializeBinary(const IColumn & column...
$CLICKHOUSE_LOCAL --query "
    SELECT 'RowBinary';
    SET engine_file_truncate_on_insert = 1;
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(Float32, 7)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7]);
    INSERT INTO qbit VALUES (2, [9, 10, 11, 12, 13, 14, 15]);
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_1.clickhouse', 'RowBinary') SELECT * FROM qbit;
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_1.clickhouse', 'RowBinary', 'id UInt32, vec QBit(Float32, 7)') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"


# ========== Test Clickhouse Client ==========
$CLICKHOUSE_LOCAL --query "SELECT '==========CLIENT===========';"

# [de]serializeText(const IColumn & column...)
$CLICKHOUSE_CLIENT --query "
    SELECT 'Normal';
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(Float32, 7)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7]);
    INSERT INTO qbit VALUES (2, [9, 10, 11, 12, 13, 14, 15]);
    SELECT * FROM qbit ORDER BY id;
"

# Native format: [de]serializeBinaryBulkWithMultipleStreams
$CLICKHOUSE_CLIENT --query "
    SELECT 'Native';
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native') SELECT * FROM qbit;
    DESCRIBE file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native');
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
"

# RowBinary format: [de]serializeBinaryBulkWithMultipleStreams
$CLICKHOUSE_CLIENT --query "
    SELECT 'RowBinary';
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse', 'RowBinary') SELECT * FROM qbit;
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse', 'RowBinary', 'id UInt32, vec QBit(Float32, 7)') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"


# ========== Test BFloat16 and Float64 RowBinary ==========
$CLICKHOUSE_LOCAL --query "SELECT '==========BFloat16===========';"

$CLICKHOUSE_LOCAL --query "
    SELECT 'RowBinary';
    SET engine_file_truncate_on_insert = 1;
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 4)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1.5, 2.5, 3.5, 4.5]);
    INSERT INTO qbit VALUES (2, [5, 6, 7, 8]);
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_4.clickhouse', 'RowBinary') SELECT * FROM qbit;
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_4.clickhouse', 'RowBinary', 'id UInt32, vec QBit(BFloat16, 4)') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"

$CLICKHOUSE_LOCAL --query "SELECT '==========Float64===========';"

$CLICKHOUSE_LOCAL --query "
    SELECT 'RowBinary';
    SET engine_file_truncate_on_insert = 1;
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(Float64, 3)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1.1, 2.2, 3.3]);
    INSERT INTO qbit VALUES (2, [4.4, 5.5, 6.6]);
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_5.clickhouse', 'RowBinary') SELECT * FROM qbit;
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_5.clickhouse', 'RowBinary', 'id UInt32, vec QBit(Float64, 3)') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"


rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_1.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_4.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_5.clickhouse
