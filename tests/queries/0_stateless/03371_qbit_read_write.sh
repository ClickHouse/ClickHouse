#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# ========== Test Clickhouse Local ==========
$CLICKHOUSE_LOCAL --query "SELECT '==========LOCAL===========';"

$CLICKHOUSE_LOCAL --query "
    SELECT 'Normal';
    SET allow_experimental_qbit_type = 1;
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
    SET allow_experimental_qbit_type = 1;
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
    SET allow_experimental_qbit_type = 1;
    DROP TABLE IF EXISTS qbit;
    CREATE TABLE qbit (id UInt32, vec QBit(Float32, 7)) ENGINE=Memory;
    INSERT INTO qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7]);
    INSERT INTO qbit VALUES (2, [9, 10, 11, 12, 13, 14, 15]);
    SELECT * FROM qbit ORDER BY id;
"

# Native format: [de]serializeBinaryBulkWithMultipleStreams
$CLICKHOUSE_CLIENT --query "
    SELECT 'Native';
    SET allow_experimental_qbit_type = 1;
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native') SELECT * FROM qbit;
    DESCRIBE file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native');
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse', 'Native') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
"

# RowBinary format: [de]serializeBinaryBulkWithMultipleStreams
$CLICKHOUSE_CLIENT --query "
    SELECT 'RowBinary';
    SET allow_experimental_qbit_type = 1;
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse', 'RowBinary') SELECT * FROM qbit;
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse', 'RowBinary', 'id UInt32, vec QBit(Float32, 7)') ORDER BY id;
    SELECT * FROM qbit ORDER BY id;
    DROP TABLE qbit;
"


rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_1.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_2.clickhouse
rm -f ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_3.clickhouse
