-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- An enum group whose alternatives contain a wildcard, such as `file_{a?,b?}.csv`, must be listed
-- and matched, not expanded into exact keys: `expand` renders the `?` as literal text, so expanding
-- would probe objects literally named `file_a?.csv`. With `use_glob_ast_parser = 1` the result must
-- be the same as with the legacy parser.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/enum_wildcard/file_a1.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 1;
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/enum_wildcard/file_b2.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 2;

SELECT c1 FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/enum_wildcard/file_{a?,b?}.csv', format = CSV) ORDER BY c1
SETTINGS use_glob_ast_parser = 1, s3_ignore_file_doesnt_exist = 0;

SELECT c1 FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/enum_wildcard/file_{a?,b?}.csv', format = CSV) ORDER BY c1
SETTINGS use_glob_ast_parser = 0, s3_ignore_file_doesnt_exist = 0;

-- Both queries also exercise schema inference and `getPathSample`, which has the same
-- expansion fast path, because no structure is given.
