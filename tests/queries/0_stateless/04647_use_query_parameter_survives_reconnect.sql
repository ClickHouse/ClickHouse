-- `USE {db:Identifier}` keeps the database name in a query parameter, so the client has to
-- substitute the parameters to learn the new database name. Otherwise it remembers an empty
-- default database and silently resets the current database when it re-establishes a connection.

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.t_04647;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_04647 (x UInt8) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t_04647 VALUES (1);

USE {CLICKHOUSE_DATABASE:Identifier};

-- Ask the server to close the connection as soon as it becomes idle, so that the queries below
-- are executed in a new session, established by the client on its own.
SET idle_connection_timeout = 0;

SELECT currentDatabase() = {CLICKHOUSE_DATABASE:String}, sum(x) FROM t_04647;
SELECT currentDatabase() = {CLICKHOUSE_DATABASE:String}, sum(x) FROM t_04647;
