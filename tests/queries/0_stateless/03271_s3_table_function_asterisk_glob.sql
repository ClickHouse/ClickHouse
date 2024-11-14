-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

SET max_threads = 1;
SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/', format=Parquet) SELECT 0 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file1', format=Parquet) SELECT 1 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file2', format=Parquet) SELECT 2 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file3', format=Parquet) SELECT 3 as num;

SELECT * FROM s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL;
