-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- Classification of persisted table metadata must not depend on the per-query
-- `use_glob_ast_parser` setting: to the AST parser a literal brace group like `{x}` is
-- constant text (not a glob), but letting that reclassification through would make the same
-- stored path flip between readonly and writable (or between valid and invalid for
-- `PARTITION BY`) across sessions. The readonly guard for writes/truncate and the
-- partition-strategy validation always use the legacy classification.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

SET use_glob_ast_parser = 1;

-- The write readonly guard: a path with a literal brace group stays readonly.
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/' || currentDatabase() || '/metadata_guard/data_{x}.csv', format = CSV) SELECT 1; -- { serverError DATABASE_ACCESS_DENIED }

DROP TABLE IF EXISTS t_glob_ast_readonly;
CREATE TABLE t_glob_ast_readonly (c1 Int32) ENGINE = S3(s3_conn, url = 'http://localhost:11111/test/metadata_guard/data_{x}.csv', format = CSV);
INSERT INTO t_glob_ast_readonly SELECT 1; -- { serverError DATABASE_ACCESS_DENIED }
TRUNCATE TABLE t_glob_ast_readonly; -- { serverError DATABASE_ACCESS_DENIED }
DROP TABLE t_glob_ast_readonly;

-- Partition-strategy validation: a globbed path (by legacy classification) is rejected for
-- hive partitioning regardless of the setting.
CREATE TABLE t_glob_ast_partitioned (c1 Int32) ENGINE = S3(s3_conn, url = 'http://localhost:11111/test/metadata_guard/dir_{x}/file.parquet', format = Parquet, partition_strategy = 'hive') PARTITION BY c1; -- { serverError BAD_ARGUMENTS }

-- The queue engine validates its path with the legacy classification too: a literal brace
-- group still counts as a glob, so the table is accepted (not rejected as "must contain
-- globs") the same way in every session.
-- No explicit `keeper_path`: the default path includes the table UUID, so repeated runs
-- of this test cannot race on the cleanup of a shared Keeper node.
DROP TABLE IF EXISTS t_glob_ast_queue;
CREATE TABLE t_glob_ast_queue (c1 Int32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/metadata_guard/queue_{x}.csv', 'username', 'password', CSV)
SETTINGS mode = 'unordered';
DROP TABLE t_glob_ast_queue;
