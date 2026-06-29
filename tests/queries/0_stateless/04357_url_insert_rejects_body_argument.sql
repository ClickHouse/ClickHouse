-- The `body(...)` argument forms the HTTP request body when reading from the `url` table function.
-- For `INSERT INTO FUNCTION url(...)` the inserted rows are sent as the request body instead, so a
-- user-provided `body` argument would be silently dropped. It must be rejected loudly.
-- The error is thrown during storage creation, so no connection to the URL is made.
-- The same applies to the `urlCluster` sibling: without this guard an omitted structure would even
-- send a body `POST` for schema inference before the insert failed.

SET enable_analyzer = 1;
INSERT INTO FUNCTION url('http://localhost:11111/test/data', body('payload')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION url('http://localhost:11111/test/data', 'TSV', body((SELECT 1))) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION url('http://localhost:11111/test/data', body('')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', body('payload')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'TSV', body((SELECT 1))) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', body('')) SELECT 1; -- { serverError BAD_ARGUMENTS }

SET enable_analyzer = 0;
INSERT INTO FUNCTION url('http://localhost:11111/test/data', body('payload')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION url('http://localhost:11111/test/data', 'TSV', body((SELECT 1))) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION url('http://localhost:11111/test/data', body('')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', body('payload')) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'TSV', body((SELECT 1))) SELECT 1; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', body('')) SELECT 1; -- { serverError BAD_ARGUMENTS }
