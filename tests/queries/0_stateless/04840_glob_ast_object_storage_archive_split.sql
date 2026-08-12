-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- The archive-vs-plain split of object-storage paths (`archive::inner`) must follow the
-- selected glob parser, like the `file` engine (04826). The discriminating pattern is
-- `data_{x}_{y}`: to the AST parser both single-char brace groups are constant text, so
-- nothing on the left of `::` in `data_{x}_{y}::foo.csv` is a glob and the whole string
-- stays one exact key; the legacy parser sees `{` and splits it into the archive glob
-- `data_{x}_{y}` plus the inner path `foo.csv` (and with two brace groups it cannot take
-- the exact-key expansion shortcut either, so it lists and matches nothing).
-- https://github.com/ClickHouse/ClickHouse/pull/91062

-- Legacy parser: split into an archive glob that matches no objects; 0 rows.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}_{y}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 0;

-- AST parser: the exact key `data_{x}_{y}::foo.csv` does not exist, so the read fails instead
-- of silently taking the archive-glob path (which would return 0 rows).
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}_{y}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 1; -- { serverError S3_ERROR }

SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}_{y}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 1, s3_ignore_file_doesnt_exist = 1;

-- A single one-element brace group `data_{x}` fails under BOTH parsers, for different
-- reasons: the legacy parser splits off the archive part `data_{x}` and then expands it
-- (exactly one brace group) to the exact key `data_x`, whose strict read fails; the AST
-- parser treats `{x}` as constant text, does not split at all, and fails reading the exact
-- key `data_{x}::foo.csv`.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 0; -- { serverError S3_ERROR }
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 1; -- { serverError S3_ERROR }
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{x}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 1, s3_ignore_file_doesnt_exist = 1;

-- Multi-element enums are globs under both parsers, so the AST parser keeps genuine
-- glob-archive syntax: `data_{a,b}_{c,d}` is split off as an archive glob under both
-- settings, and with two enums neither parser takes the exact-key expansion shortcut,
-- so both list and match nothing.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{a,b}_{c,d}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 0;
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/arch_split/data_{a,b}_{c,d}::foo.csv', format = 'CSV', structure = 'c1 Int32') SETTINGS use_glob_ast_parser = 1;

-- Stored tables always classify their persisted path with the legacy parser: even under the
-- AST setting, CREATE splits off the archive glob, and reads return 0 rows instead of failing
-- on the (absent) exact key `data_{x}_{y}::foo.csv`. DETACH/ATTACH must not reclassify it either.
SET use_glob_ast_parser = 1;
DROP TABLE IF EXISTS t_glob_ast_arch_split;
CREATE TABLE t_glob_ast_arch_split (c1 Int32) ENGINE = S3(s3_conn, filename = currentDatabase() || '/arch_split/data_{x}_{y}::foo.csv', format = 'CSV');
SELECT count() FROM t_glob_ast_arch_split;
DETACH TABLE t_glob_ast_arch_split;
ATTACH TABLE t_glob_ast_arch_split;
SELECT count() FROM t_glob_ast_arch_split;
DROP TABLE t_glob_ast_arch_split;
