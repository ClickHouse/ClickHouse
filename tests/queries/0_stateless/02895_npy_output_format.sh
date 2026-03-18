#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
chmod 777 ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

${CLICKHOUSE_CLIENT} -q --ignore-error "
    DROP DATABASE IF EXISTS npy_output_02895;
    CREATE DATABASE IF NOT EXISTS npy_output_02895;

    SELECT '-- test data types --';
    CREATE TABLE IF NOT EXISTS npy_output_02895.data_types
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
        s String
    ) Engine = MergeTree ORDER BY i1;

    INSERT INTO npy_output_02895.data_types VALUES (1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, 'npy', 'npy'), (-1, -1, -1, -1, 0, 0, 0, 0, 0.2, 0.02, 'npy', 'npynpy');

    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int8.npy') SELECT i1 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int16.npy') SELECT i2 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int32.npy') SELECT i4 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int64.npy') SELECT i8 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint8.npy') SELECT u1 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint16.npy') SELECT u2 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint32.npy') SELECT u4 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint64.npy') SELECT u8 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float32.npy') SELECT f4 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float64.npy') SELECT f8 FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_fixedstring.npy') SELECT fs FROM npy_output_02895.data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_string.npy') SELECT s FROM npy_output_02895.data_types;

    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int8.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int16.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int32.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int64.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint8.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint16.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint32.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint64.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float32.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float64.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_fixedstring.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_string.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int8.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int16.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int32.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_int64.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint8.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint16.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint32.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_uint64.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float32.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_float64.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_fixedstring.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_dtype_string.npy');

    SELECT '-- test nested data types --';
    CREATE TABLE IF NOT EXISTS npy_output_02895.nested_data_types
    (
        i4 Array(Array(Array(Int8))),
        f8 Array(Array(Float64)),
        s Array(String),
    ) Engine = MergeTree ORDER BY i4;

    INSERT INTO npy_output_02895.nested_data_types VALUES ([[[1], [2]], [[3], [4]]], [[0.1], [0.2]], ['a', 'bb']), ([[[1], [2]], [[3], [4]]], [[0.1], [0.2]], ['ccc', 'dddd']);

    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_int32.npy') SELECT i4 FROM npy_output_02895.nested_data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_float64.npy') SELECT f8 FROM npy_output_02895.nested_data_types;
    INSERT INTO TABLE FUNCTION file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_string.npy') SELECT s FROM npy_output_02895.nested_data_types;

    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_int32.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_float64.npy');
    SELECT * FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_string.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_int32.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_float64.npy');
    DESC file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/02895_nested_dtype_string.npy');

    SELECT '-- test exceptions --';
    CREATE TABLE IF NOT EXISTS npy_output_02895.exceptions
    (
        unsupported_u UInt256,
        unsupported_date Date,
        unsupported_tuple Tuple(Int16, Int16),
        unsupported_nested_i Array(Int128),
        ragged_dimention Array(Int16),
        zero_dimension Array(Int16)
    ) Engine = MergeTree ORDER BY unsupported_u;

    INSERT INTO npy_output_02895.exceptions VALUES (1, '2019-01-01', (1, 1), [1, 1], [1, 1], []), (0, '2019-01-01', (0, 0), [0, 0], [0], [0]);

    SELECT * FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError TOO_MANY_COLUMNS }
    SELECT unsupported_u FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError BAD_ARGUMENTS }
    SELECT unsupported_date FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError BAD_ARGUMENTS }
    SELECT unsupported_tuple FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError BAD_ARGUMENTS }
    SELECT unsupported_nested_i FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError BAD_ARGUMENTS }
    SELECT ragged_dimention FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError ILLEGAL_COLUMN }
    SELECT zero_dimension FROM npy_output_02895.exceptions FORMAT Npy; -- { clientError ILLEGAL_COLUMN }

    DROP DATABASE IF EXISTS npy_output_02895;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
