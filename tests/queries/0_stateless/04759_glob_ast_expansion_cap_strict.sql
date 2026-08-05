-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- `glob_expansion_max_elements` is a resource cap, not permission to change semantics: a
-- single-enum glob with `use_glob_ast_parser = 1` must not silently fall back from the
-- exact-key path (`KeysIterator`, which throws on a missing key when
-- `s3_ignore_file_doesnt_exist = 0`) to the listing path (which skips missing keys) when the
-- enum's cardinality exceeds the cap. Exceeding the cap throws `BAD_ARGUMENTS` instead.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/expansion_cap/file_a.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 1;

-- Under the cap: exact keys are probed, so the absent `file_b.csv` throws in strict mode.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/expansion_cap/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 1, glob_expansion_max_elements = 10, s3_ignore_file_doesnt_exist = 0; -- { serverError S3_ERROR }

-- Over the cap: the read must not degrade to list-and-filter (which would silently return 1
-- row, skipping the missing alternative); it throws instead.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/expansion_cap/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 1, glob_expansion_max_elements = 1, s3_ignore_file_doesnt_exist = 0; -- { serverError BAD_ARGUMENTS }

-- Over the cap in non-strict mode as well: the cap is enforced uniformly.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/expansion_cap/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 1, glob_expansion_max_elements = 1, s3_ignore_file_doesnt_exist = 1; -- { serverError BAD_ARGUMENTS }

-- Sanity check: under the cap with `s3_ignore_file_doesnt_exist = 1` the read succeeds.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/expansion_cap/file_{a,b}.csv', format = 'CSV', structure = 'c1 Int32')
SETTINGS use_glob_ast_parser = 1, glob_expansion_max_elements = 10, s3_ignore_file_doesnt_exist = 1;
