-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET enable_time_time64_type = 1, session_timezone = 'UTC';

DROP TABLE IF EXISTS paimon_all_types_storage;

CREATE TABLE paimon_all_types_storage
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

DESC paimon_all_types_storage;

SELECT '===';

SELECT
    f_boolean,
    f_char,
    f_varchar,
    f_string,
    f_binary,
    f_varbinary,
    f_bytes,
    f_decimal,
    f_decimal2,
    f_decimal3,
    f_tinyint,
    f_smallint,
    f_int,
    f_bigint,
    f_float,
    f_double,
    f_date,
    f_time,
    f_timestamp,
    f_timestamp2,
    toTimeZone(f_timestamp3, 'Asia/Shanghai'),
    f_boolean_nn,
    f_char_nn,
    f_varchar_nn,
    f_string_nn,
    f_binary_nn,
    f_varbinary_nn,
    f_bytes_nn,
    f_decimal_nn,
    f_decimal2_nn,
    f_decimal3_nn,
    f_tinyint_nn,
    f_smallint_nn,
    f_int_nn,
    f_bigint_nn,
    f_float_nn,
    f_double_nn,
    f_date_nn,
    f_time_nn,
    f_timestamp_nn,
    f_timestamp2_nn,
    toTimeZone(f_timestamp3_nn, 'Asia/Shanghai'),
    f_array,
    f_map
FROM paimon_all_types_storage
ORDER BY f_int_nn;

SELECT '===';

SELECT count(1) FROM paimon_all_types_storage;

DROP TABLE IF EXISTS paimon_all_types_storage;

