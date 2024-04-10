#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_files_path=$($CLICKHOUSE_CLIENT_BINARY -q "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
chmod 777 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

${CLICKHOUSE_CLIENT} -n -q "
    DROP DATABASE IF EXISTS npy_output_02895;
    CREATE DATABASE IF NOT EXISTS npy_output_02895;

    CREATE TABLE IF NOT EXISTS npy_output_02895.common
    (
        i1 Int8,
        i2 Int16,
        i4 Int32,
        i8 Int64,
        u1 UInt8,
        u2 UInt16,
        u4 UInt32,
        u8 UInt64,
        f4 Float32,
        f8 Float64,
        fs FixedString(10),
        s String,
        unknow Int128
    ) Engine = MergeTree ORDER BY i1;

    INSERT INTO npy_output_02895.common VALUES (-1,-1,-1,-1,1,1,1,1,0.1,0.01,'npy','npy',1), (-1,-1,-1,-1,1,1,1,1,0.1,0.01,'npy','npy',1), (-1,-1,-1,-1,1,1,1,1,0.1,0.01,'npy','npy',1);

    SELECT * FROM npy_output_02895.common FORMAT Npy; -- { clientError TOO_MANY_COLUMNS }
    SELECT unknow FROM npy_output_02895.common FORMAT Npy; -- { clientError BAD_ARGUMENTS }

    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int8.npy') SELECT i1 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int16.npy') SELECT i2 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int32.npy') SELECT i4 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int64.npy') SELECT i8 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint8.npy') SELECT u1 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint16.npy') SELECT u2 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint32.npy') SELECT u4 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint64.npy') SELECT u8 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float32.npy') SELECT f4 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float64.npy') SELECT f8 FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_fixedstring.npy') SELECT fs FROM npy_output_02895.common;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_string.npy') SELECT s FROM npy_output_02895.common;

    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int8.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int16.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int32.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int64.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint8.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint16.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint32.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint64.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float32.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float64.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_fixedstring.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_string.npy');

    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int8.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int16.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int32.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_int64.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint8.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint16.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint32.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_uint64.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float32.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_float64.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_fixedstring.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_common_string.npy');

    CREATE TABLE IF NOT EXISTS npy_output_02895.nested
    (
        i4 Array(Array(Array(Int8))),
        f8 Array(Array(Float64)),
        s Array(Array(String)),
        unknow Array(Int128),
        ragged_1 Array(Array(Int32)),
        ragged_2 Array(Array(Int32))
    ) Engine = MergeTree ORDER BY i4;

    INSERT INTO npy_output_02895.nested VALUES ([[[1], [2]], [[3], [4]]], [[0.1], [0.2]], [['a', 'bb'], ['ccc', 'dddd']], [1, 2], [[1, 2], [3, 4]], [[1, 2], [3]]), ([[[1], [2]], [[3], [4]]], [[0.1], [0.2]], [['a', 'bb'], ['ccc', 'dddd']], [1, 2], [[1, 2, 3], [4]], [[1, 2], [3]]), ([[[1], [2]], [[3], [4]]], [[0.1], [0.2]], [['a', 'bb'], ['ccc', 'dddd']], [1, 2], [[1], [2, 3, 4]], [[1, 2], [3]]);

    SELECT * FROM npy_output_02895.nested FORMAT Npy; -- { clientError TOO_MANY_COLUMNS }
    SELECT unknow FROM npy_output_02895.nested FORMAT Npy; -- { clientError BAD_ARGUMENTS }
    SELECT ragged_1 FROM npy_output_02895.nested FORMAT Npy; -- { clientError ILLEGAL_COLUMN }
    SELECT ragged_2 FROM npy_output_02895.nested FORMAT Npy; -- { clientError ILLEGAL_COLUMN }

    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_int32.npy') SELECT i4 FROM npy_output_02895.nested;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_float64.npy') SELECT f8 FROM npy_output_02895.nested;
    INSERT INTO TABLE FUNCTION file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_string.npy') SELECT s FROM npy_output_02895.nested;

    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_int32.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_float64.npy');
    SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_string.npy');

    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_int32.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_float64.npy');
    DESC file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_string.npy');

    DROP DATABASE IF EXISTS npy_output_02895;"

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
