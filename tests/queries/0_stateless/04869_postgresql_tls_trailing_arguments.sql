-- Tags: no-fasttest
-- no-fasttest: the PostgreSQL integration is not available in the fast test build.

-- A TLS parameter of a PostgreSQL source given twice in the trailing key-value arguments must be
-- rejected, and the trailing arguments must not count towards the positional ones.

DROP TABLE IF EXISTS t_04869;
DROP DATABASE IF EXISTS db_04869;
DROP TABLE IF EXISTS t_04869;

SELECT * FROM postgresql('127.0.0.1:1', 'd', 't', 'u', 'p', sslmode = 'require', sslmode = 'disable') ORDER BY 1; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE db_04869 ENGINE = PostgreSQL('127.0.0.1:1', 'd', 'u', 'p', sslmode = 'require', sslmode = 'disable'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_04869 (x Int32) ENGINE = PostgreSQL('127.0.0.1:1', 'd', 't', 'u', 'p', sslrootcert_pem = 'a', sslrootcert_pem = 'b'); -- { serverError BAD_ARGUMENTS }

CREATE DATABASE db_04869 ENGINE = PostgreSQL('127.0.0.1:1', 'd', 'u', 'p', 'sch', 1, sslmode = 'require');
SELECT engine FROM system.databases WHERE name = 'db_04869' ORDER BY 1;

CREATE TABLE t_04869 (x Int32) ENGINE = PostgreSQL('127.0.0.1:1', 'd', 't', 'u', 'p', 'sch', sslmode = 'require');
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_04869' ORDER BY 1;

DROP TABLE t_04869;
DROP DATABASE db_04869;
DROP TABLE IF EXISTS t_04869;
