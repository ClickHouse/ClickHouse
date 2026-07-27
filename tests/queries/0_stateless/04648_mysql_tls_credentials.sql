-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-fasttest: the MySQL table engine and the `mysql` table function are not in the fast test build.
-- no-parallel, no-replicated-database: a named collection is used.

DROP NAMED COLLECTION IF EXISTS 04648_mysql_with_path;
DROP NAMED COLLECTION IF EXISTS 04648_mysql;
DROP TABLE IF EXISTS 04648_table;

-- A certificate or key path names a file that the server opens with its own privileges, so it may
-- only be specified in the server configuration file. In a named collection created with SQL it is
-- rejected, and the contents have to be passed in `ssl_ca_pem`, `ssl_cert_pem` or `ssl_key_pem`.

CREATE NAMED COLLECTION 04648_mysql_with_path AS
    host = '127.0.0.1', port = 3306, user = 'u', password = 'p', database = 'd', ssl_ca = '/etc/ssl/certs/ca.crt';

SELECT * FROM mysql(04648_mysql_with_path, table = 't'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE 04648_table (a Int) ENGINE = MySQL(04648_mysql_with_path, table = 't'); -- { serverError BAD_ARGUMENTS }

CREATE NAMED COLLECTION 04648_mysql AS
    host = '127.0.0.1', port = 3306, user = 'u', password = 'p', database = 'd';

-- Overriding a path in a query is rejected as well, so that a collection without one cannot be used
-- to make the server read an arbitrary file.
SELECT * FROM mysql(04648_mysql, table = 't', ssl_ca = '/etc/ssl/certs/ca.crt'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mysql(04648_mysql, table = 't', ssl_cert = '/etc/ssl/certs/client.crt'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mysql(04648_mysql, table = 't', ssl_key = '/etc/ssl/private/client.key'); -- { serverError BAD_ARGUMENTS }

-- The contents are accepted from a query. Nothing is connected to at `CREATE`, which is why this
-- works without a MySQL server. `table` is deliberately in between two of the credentials.
CREATE TABLE 04648_table (a Int) ENGINE = MySQL(04648_mysql,
    ssl_ca_pem = '-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n',
    table = 't',
    ssl_key_pem = '-----BEGIN PRIVATE KEY-----\nSUPERSECRET04648\n-----END PRIVATE KEY-----\n');

-- The credentials are masked like a password is, and the arguments in between stay visible.
SELECT
    extract(create_table_query, 'ssl_ca_pem = \'[^\']*\''),
    extract(create_table_query, 'table = \'[^\']*\''),
    extract(create_table_query, 'ssl_key_pem = \'[^\']*\'')
FROM system.tables WHERE database = currentDatabase() AND name = '04648_table';

SYSTEM FLUSH LOGS query_log;

-- The literal is split so that this query does not match itself once it is logged in turn.
SELECT count() FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
  AND query LIKE '%SUPERSECRET' || '04648%';

DROP TABLE 04648_table;
DROP NAMED COLLECTION 04648_mysql;
DROP NAMED COLLECTION 04648_mysql_with_path;
