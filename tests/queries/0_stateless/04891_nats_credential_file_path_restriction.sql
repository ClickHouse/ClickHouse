-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag justification:
--   no-fasttest: the NATS engine is not available in a build without libraries.
--   no-parallel: creates and drops global named collections; the flaky check runs the same test
--                concurrently, and parallel repetitions collide on them (the first finishing run
--                drops the collection while others still use it).
--   no-replicated-database: named collections are server-global, not database-scoped.

-- `nats_credential_file` is a path on the server filesystem: the server opens it with its own
-- privileges, and during authentication the credentials are sent to `nats_url`, which comes from
-- the same query. A path taken from SQL would let anyone who can define a `NATS` source probe the
-- local filesystem and exfiltrate files the server can read to a NATS server they control. So the
-- path is accepted only from a named collection defined in the server configuration file, or as
-- `nats.credential_file` in the server configuration itself; every SQL spelling is rejected, and
-- the inline `nats_credentials` setting is the SQL way to provide the same credentials.
-- The tables never connect to a NATS server: `127.0.0.1:1` (used by the `nats_config_credentials`
-- collection from the test server configuration as well) refuses the connection immediately,
-- so passing the validation surfaces as `CANNOT_CONNECT_NATS`.

-- The path in the `SETTINGS` clause of the query.
CREATE TABLE nats_file_in_settings (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    nats_credential_file = '/etc/passwd'; -- { serverError BAD_ARGUMENTS }

-- The path stored in a named collection created by SQL: `CREATE NAMED COLLECTION` does not require
-- the privileges needed to read arbitrary server files, so this source is not trusted either.
DROP NAMED COLLECTION IF EXISTS 04891_nats_sql_collection;
CREATE NAMED COLLECTION 04891_nats_sql_collection AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_credential_file = '/etc/passwd';

CREATE TABLE nats_file_in_sql_collection (key UInt64) ENGINE = NATS(04891_nats_sql_collection); -- { serverError BAD_ARGUMENTS }

-- The path as an engine-argument override on top of a named collection.
CREATE TABLE nats_file_in_override (key UInt64)
ENGINE = NATS(04891_nats_sql_collection, nats_credential_file = '/etc/shadow'); -- { serverError BAD_ARGUMENTS }

-- Replacing the path stored in the SQL collection with inline credentials from the query is fine:
-- the path itself is never used then.
CREATE TABLE nats_credentials_over_sql_file (key UInt64)
ENGINE = NATS(04891_nats_sql_collection, nats_credentials = 'user JWT and seed'); -- { serverError CANNOT_CONNECT_NATS }

DROP NAMED COLLECTION 04891_nats_sql_collection;

-- A named collection defined in the server configuration file is operator-controlled,
-- so a path stored there is accepted: the validation passes and the connection attempt fails.
CREATE TABLE nats_file_in_config_collection (key UInt64) ENGINE = NATS(nats_config_credentials); -- { serverError CANNOT_CONNECT_NATS }

-- But an override of the path on top of the configuration-defined collection comes from SQL again.
CREATE TABLE nats_file_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credential_file = '/etc/shadow'); -- { serverError BAD_ARGUMENTS }

-- The `SETTINGS` clause spelling of the same override is query-level as well.
CREATE TABLE nats_file_in_settings_over_config_collection (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_credential_file = '/etc/shadow'; -- { serverError BAD_ARGUMENTS }

-- Replacing the configured path with inline credentials from the query is fine.
CREATE TABLE nats_credentials_over_config_file (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = 'user JWT and seed'); -- { serverError CANNOT_CONNECT_NATS }
