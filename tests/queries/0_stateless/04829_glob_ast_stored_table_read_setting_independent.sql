-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- A stored table must interpret its persisted path the same way in every session: to the AST
-- parser a literal brace group like `{x}` is constant text, so letting the per-query
-- `use_glob_ast_parser` setting through would make schema/format inference and reads of the
-- same stored path flip between the literal key `data_{x}.jsonl` and the legacy expansion
-- `data_x.jsonl` across sessions. Stored-table inference and reads always use the legacy
-- classification; only per-query paths (table functions, `INFILE`) follow the setting.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/store_pin/data_x.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 1 AS c1;

SET use_glob_ast_parser = 1;

DROP TABLE IF EXISTS t_glob_ast_stored;

-- Schema/format inference at CREATE must expand `{x}` like the legacy parser and read
-- `data_x.jsonl`, not probe the (absent) literal key `data_{x}.jsonl`.
CREATE TABLE t_glob_ast_stored ENGINE = S3(s3_conn, filename = currentDatabase() || '/store_pin/data_{x}.jsonl');

-- Reads return the same rows regardless of the session setting.
SELECT c1 FROM t_glob_ast_stored;
SET use_glob_ast_parser = 0;
SELECT c1 FROM t_glob_ast_stored;

-- Re-attaching under the AST setting must not reclassify the stored path either.
SET use_glob_ast_parser = 1;
DETACH TABLE t_glob_ast_stored;
ATTACH TABLE t_glob_ast_stored;
SELECT c1 FROM t_glob_ast_stored;

-- The per-query table function keeps the AST classification: the same pattern is the
-- literal key `data_{x}.jsonl`, which does not exist.
SELECT count() FROM s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/store_pin/data_{x}.jsonl', format = 'JSONEachRow', structure = 'c1 Int32') SETTINGS s3_ignore_file_doesnt_exist = 1;

DROP TABLE t_glob_ast_stored;
