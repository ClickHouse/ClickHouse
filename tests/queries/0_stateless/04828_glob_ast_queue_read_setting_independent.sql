-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3 (minio) and the `S3Queue` engine

-- The `S3Queue` read path must classify the persisted path with the legacy glob parser
-- regardless of `use_glob_ast_parser`, matching the setting-independent path validation at
-- CREATE / ATTACH: to the AST parser a literal brace group like `{x}` is constant text (not
-- a glob), so an accepted table with such a path would otherwise load fine and then fail
-- every background poll or direct read with "Using glob iterator with path without globs"
-- as soon as the setting is enabled.
-- https://github.com/ClickHouse/ClickHouse/pull/91062

SET use_glob_ast_parser = 1;
SET stream_like_engine_allow_direct_select = 1;

-- No explicit `keeper_path`: the default path includes the table UUID, so repeated runs
-- of this test cannot race on the cleanup of a shared Keeper node.
DROP TABLE IF EXISTS t_glob_ast_queue_read;
CREATE TABLE t_glob_ast_queue_read (c1 Int32)
ENGINE = S3Queue('http://localhost:11111/test/04828_queue_glob/queue_{x}.csv', 'test', 'testtest', CSV)
SETTINGS mode = 'unordered';

-- The prefix is never written to, so the listing is empty and the read returns 0 rows,
-- exactly as with the legacy parser. Before the fix this threw BAD_ARGUMENTS.
SELECT count() FROM t_glob_ast_queue_read;

DROP TABLE t_glob_ast_queue_read;
