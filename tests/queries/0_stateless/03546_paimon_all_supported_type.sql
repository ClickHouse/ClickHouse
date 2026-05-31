-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS
set enable_time_time64_type=1, session_timezone='UTC';

desc paimonS3(s3_conn, filename='paimon_all_types');

select '===';

SELECT f_boolean,
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
FROM paimonS3(s3_conn, filename='paimon_all_types') ORDER BY f_int_nn;

select '===';
SELECT count(1) FROM paimonS3(s3_conn, filename='paimon_all_types');
