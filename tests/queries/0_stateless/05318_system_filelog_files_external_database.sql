-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: the MySQL database engine is not built in fast test.
-- Tag no-parallel: the MySQL database points at a closed port, so a concurrent scan of system.tables without a
-- database filter would fail on it (as in 04506_remote_database_unreachable_no_error_log).

-- system.filelog_files does not read the tables of databases of external engines, which cannot contain FileLog
-- tables, so an unreachable MySQL server does not make it fail.

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password') SETTINGS connect_timeout = 1, connection_max_tries = 1;

SELECT count() FROM system.filelog_files WHERE database = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
