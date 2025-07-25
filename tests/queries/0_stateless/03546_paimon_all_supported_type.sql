-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

desc paimonS3(s3_conn, filename='paimon_all_types');
select '===';
SELECT * FROM paimonS3(s3_conn, filename='paimon_all_types') ORDER BY f_int_nn;
select '===';
SELECT count(1) FROM paimonS3(s3_conn, filename='paimon_all_types');
