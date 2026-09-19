-- Tags: no-fasttest
-- Tag justification:
--   no-fasttest: depends on libpqxx (the `PostgreSQL` table engine), which is not built in fast test.
--
-- The `PostgreSQL` engine resolves its settings into a connection pool and keeps none of them, so it used to
-- report nothing at all. It now keeps the enumeration made there. The creator overlays the `SETTINGS` clause
-- on the creating session's values, and all three sources are told apart: `definition` for what the clause
-- states, `other` for what the session held, `default` for the rest.
--
-- The engine does not connect at `CREATE` time when the columns are given explicitly and the connection pool
-- is created lazily, so an unreachable host is fine here: nothing is ever connected.

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_pg;

SET postgresql_connection_pool_retries = 7;

CREATE TABLE t_pg (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS postgresql_connection_pool_size = 8;

SELECT '-- the clause, the session and the default are told apart';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_pg'
ORDER BY name;

DROP TABLE t_pg;
