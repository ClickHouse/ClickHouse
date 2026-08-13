-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- With `use_glob_ast_parser = 1`, a glob with several enum groups where some generated keys are
-- missing must keep the legacy listing behavior: return the keys that do exist instead of
-- failing on the first absent cartesian-product member while `s3_ignore_file_doesnt_exist = 0`.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/multi_enum/file_a1.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 1;
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/multi_enum/file_b2.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 2;

-- file_a2.csv and file_b1.csv intentionally do not exist.
SELECT c1 FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/multi_enum/file_{a,b}{1,2}.csv', format = CSV) ORDER BY c1
SETTINGS use_glob_ast_parser = 1, s3_ignore_file_doesnt_exist = 0;

-- The legacy parser must return the same result for the same pattern.
SELECT c1 FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/multi_enum/file_{a,b}{1,2}.csv', format = CSV) ORDER BY c1
SETTINGS use_glob_ast_parser = 0, s3_ignore_file_doesnt_exist = 0;
