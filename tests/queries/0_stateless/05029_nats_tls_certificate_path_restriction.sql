-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag justification:
--   no-fasttest: the NATS engine is not available in a build without libraries.
--   no-parallel: creates and drops global named collections; the flaky check runs the same test
--                concurrently, and parallel repetitions collide on them (the first finishing run
--                drops the collection while others still use it).
--   no-replicated-database: named collections are server-global, not database-scoped.

-- The server opens the TLS paths itself, so like `nats_credential_file` they are accepted only from
-- a named collection defined in the server configuration file, whose destination SQL cannot override.
-- The paths below are the server's own certificate and key, so they load: a definition which passes
-- the policy reaches the connection attempt and fails with `CANNOT_CONNECT_NATS` against `127.0.0.1:1`.

-- The path in the `SETTINGS` clause of the query.
CREATE TABLE nats_ca_in_settings (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_secure = 1,
    nats_ca_file = '/etc/clickhouse-server/server.crt'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_client_key_in_settings (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_secure = 1,
    nats_client_cert_file = '/etc/clickhouse-server/server.crt',
    nats_client_key_file = '/etc/clickhouse-server/server.key'; -- { serverError BAD_ARGUMENTS }

-- The path stored in a named collection created by SQL: `CREATE NAMED COLLECTION` does not require
-- the privileges needed to read arbitrary server files, so this source is not trusted either.
DROP NAMED COLLECTION IF EXISTS 05029_nats_sql_collection;
CREATE NAMED COLLECTION 05029_nats_sql_collection AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_secure = 1,
    nats_ca_file = '/etc/clickhouse-server/server.crt';

CREATE TABLE nats_ca_in_sql_collection (key UInt64) ENGINE = NATS(05029_nats_sql_collection); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION 05029_nats_sql_collection;

-- A named collection defined in the server configuration file is operator-controlled, so the paths
-- stored there are accepted: the certificates load and the connection attempt fails.
CREATE TABLE nats_certificates_in_config_collection (key UInt64) ENGINE = NATS(nats_config_certificates); -- { serverError CANNOT_CONNECT_NATS }

-- An override of a configured path comes from SQL again, in either spelling.
CREATE TABLE nats_ca_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_certificates, nats_ca_file = '/etc/clickhouse-server/server.crt'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_ca_in_settings_over_config_collection (key UInt64) ENGINE = NATS(nats_config_certificates)
SETTINGS nats_ca_file = '/etc/clickhouse-server/server.crt'; -- { serverError BAD_ARGUMENTS }

-- Overriding with the empty string carries no path of its own, but it drops the one the operator
-- configured, so it is rejected on provenance rather than on the resulting value.
CREATE TABLE nats_empty_ca_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_certificates, nats_ca_file = ''); -- { serverError BAD_ARGUMENTS }

-- The operator's certificates must stay paired with the operator's destination. Both query
-- spellings of a destination override are rejected before the server attempts to connect.
CREATE TABLE nats_certificates_with_url_override (key UInt64)
ENGINE = NATS(nats_config_certificates, nats_url = 'nats://attacker:4222'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_certificates_with_server_list_override (key UInt64) ENGINE = NATS(nats_config_certificates)
SETTINGS nats_server_list = 'nats://attacker:4222'; -- { serverError BAD_ARGUMENTS }

-- A full-definition `ATTACH` is fresh user input and is validated like `CREATE`.
ATTACH TABLE nats_certificates_attach_with_url_override UUID 'c6d2423a-9ab2-4a37-8e56-10e479542001' (key UInt64)
ENGINE = NATS(nats_config_certificates, nats_url = 'nats://attacker:4222'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS nats_certificates_attach_with_url_override;

-- SQL named collections stay mutable after a table is created. Create a table while its collection
-- carries no certificates, then replay its stored metadata with a short `ATTACH` after adding a
-- path. The current contents must be validated on every reload; otherwise changing the collection
-- afterwards would bypass the restriction.
CREATE NAMED COLLECTION 05029_nats_existing_sql_collection AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 0, nats_reconnect_wait = 1;
-- A full-definition `ATTACH` tolerates a failing connection attempt, so the table exists afterwards
-- and its metadata can be replayed. The `SETTINGS` clause carries an unrelated key on purpose: an
-- engine definition whose settings are all inherited from the named collection is stored with an
-- empty `SETTINGS` clause, which the metadata reload then fails to parse.
ATTACH TABLE nats_certificates_from_existing_sql_collection UUID 'c6d2423a-9ab2-4a37-8e56-10e479542002' (key UInt64)
ENGINE = NATS(05029_nats_existing_sql_collection)
SETTINGS nats_num_consumers = 1;
DETACH TABLE nats_certificates_from_existing_sql_collection;
ALTER NAMED COLLECTION 05029_nats_existing_sql_collection
    SET nats_secure = 1, nats_ca_file = '/etc/clickhouse-server/server.crt';
ATTACH TABLE nats_certificates_from_existing_sql_collection; -- { serverError BAD_ARGUMENTS }
ALTER NAMED COLLECTION 05029_nats_existing_sql_collection DELETE nats_ca_file;
ATTACH TABLE nats_certificates_from_existing_sql_collection;
DROP TABLE nats_certificates_from_existing_sql_collection;
DROP NAMED COLLECTION 05029_nats_existing_sql_collection;

-- A certificate without its key, and certificates without `nats_secure`, are rejected wherever
-- they come from: neither can produce a working TLS connection.
DROP NAMED COLLECTION IF EXISTS 05029_nats_certificate_without_key;
CREATE NAMED COLLECTION 05029_nats_certificate_without_key AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_secure = 1,
    nats_client_cert_file = '/etc/clickhouse-server/server.crt';
CREATE TABLE nats_certificate_without_key (key UInt64) ENGINE = NATS(05029_nats_certificate_without_key); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION 05029_nats_certificate_without_key;

DROP NAMED COLLECTION IF EXISTS 05029_nats_certificates_without_secure;
CREATE NAMED COLLECTION 05029_nats_certificates_without_secure AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    nats_ca_file = '/etc/clickhouse-server/server.crt';
CREATE TABLE nats_certificates_without_secure (key UInt64) ENGINE = NATS(05029_nats_certificates_without_secure); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION 05029_nats_certificates_without_secure;
