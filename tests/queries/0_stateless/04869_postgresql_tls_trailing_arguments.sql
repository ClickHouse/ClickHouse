-- Tags: no-fasttest, need-query-parameters
-- no-fasttest: the PostgreSQL integration is not available in the fast test build.

-- A TLS parameter of a PostgreSQL source given twice in the trailing key-value arguments must be
-- rejected, and the trailing arguments must not count towards the positional ones.
-- The database name comes from the CLICKHOUSE_DATABASE_1 macro: the flaky check runs this test
-- many times in parallel, and a fixed name collides across runs.

DROP TABLE IF EXISTS t_04869;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

SELECT * FROM postgresql('127.0.0.1:1', 'd', 't', 'u', 'p', sslmode = 'require', sslmode = 'disable') ORDER BY 1; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = PostgreSQL('127.0.0.1:1', 'd', 'u', 'p', sslmode = 'require', sslmode = 'disable'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_04869 (x Int32) ENGINE = PostgreSQL('127.0.0.1:1', 'd', 't', 'u', 'p', sslrootcert_pem = 'a', sslrootcert_pem = 'b'); -- { serverError BAD_ARGUMENTS }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = PostgreSQL('127.0.0.1:1', 'd', 'u', 'p', 'sch', 1, sslmode = 'require');
SELECT engine FROM system.databases WHERE name = '{CLICKHOUSE_DATABASE_1:String}' ORDER BY 1;

CREATE TABLE t_04869 (x Int32) ENGINE = PostgreSQL('127.0.0.1:1', 'd', 't', 'u', 'p', 'sch', sslmode = 'require');
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_04869' ORDER BY 1;

DROP TABLE t_04869;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
