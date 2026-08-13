-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- A literal brace group like `{0}` is constant text for both glob parsers, so a pattern such as
-- `dir_{0}/file_{a,b}.csv` still holds exactly one enum. The legacy parser expands to exact keys
-- only when the enum's `{` is the only one in the pattern, so it lists and filters this shape;
-- with `use_glob_ast_parser = 1` the same pattern must also stay on the listing path instead of
-- probing exact keys, which would throw on the absent keys under `s3_ignore_file_doesnt_exist = 0`.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

-- A decoy that must not match: the pattern requires the literal text `dir_{0}`, not `dir_0`.
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/literal_brace/dir_0/file_a.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 1;

SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/literal_brace/dir_{0}/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 1, s3_ignore_file_doesnt_exist = 0, s3_throw_on_zero_files_match = 0;

-- The legacy parser must return the same result for the same pattern.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/literal_brace/dir_{0}/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 0, s3_ignore_file_doesnt_exist = 0, s3_throw_on_zero_files_match = 0;
