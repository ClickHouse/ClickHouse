-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag justification:
--   no-fasttest: the NATS engine is not available in a build without libraries.
--   no-parallel: creates and drops global named collections; the flaky check runs the same test
--                concurrently, and parallel repetitions collide on them (the first finishing run
--                drops the collection while others still use it).
--   no-replicated-database: named collections are server-global, not database-scoped.

-- `nats_credential_file` and `nats_credentials` are two ways to provide the same credentials,
-- so only one of them may be specified. A source specified in the query replaces a source coming
-- from the named collection instead of conflicting with it, otherwise a named collection with one
-- of the sources could not be reused by a table which provides the other one.
-- The tables never connect to a NATS server: `127.0.0.1:1` refuses the connection immediately,
-- so a successful validation surfaces as `CANNOT_CONNECT_NATS`.

DROP NAMED COLLECTION IF EXISTS 04665_nats_credential_file;
DROP NAMED COLLECTION IF EXISTS 04665_nats_credentials;
DROP NAMED COLLECTION IF EXISTS 04665_nats_both;

CREATE NAMED COLLECTION 04665_nats_credential_file AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_credential_file = '/var/nats.creds';

CREATE NAMED COLLECTION 04665_nats_credentials AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1, nats_credentials = 'user JWT and seed';

CREATE NAMED COLLECTION 04665_nats_both AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    nats_credential_file = '/var/nats.creds', nats_credentials = 'user JWT and seed';

-- Both sources specified in the query at once: ambiguous.
CREATE TABLE nats_both_in_settings (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    nats_credential_file = '/var/nats.creds', nats_credentials = 'user JWT and seed'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_both_overridden (key UInt64)
ENGINE = NATS(04665_nats_credential_file, nats_credential_file = '/var/other.creds', nats_credentials = 'user JWT and seed'); -- { serverError BAD_ARGUMENTS }

-- Both sources stored in the named collection: ambiguous as well.
CREATE TABLE nats_both_in_collection (key UInt64) ENGINE = NATS(04665_nats_both); -- { serverError BAD_ARGUMENTS }

-- A query override replaces the credential source of the named collection, in both directions.
CREATE TABLE nats_credentials_override (key UInt64)
ENGINE = NATS(04665_nats_credential_file, nats_credentials = 'user JWT and seed'); -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_credential_file_override (key UInt64)
ENGINE = NATS(04665_nats_credentials, nats_credential_file = '/var/nats.creds'); -- { serverError CANNOT_CONNECT_NATS }

-- The `SETTINGS` clause is a query-level source too, so it also replaces the collection source.
CREATE TABLE nats_credentials_in_settings (key UInt64) ENGINE = NATS(04665_nats_credential_file)
SETTINGS nats_credentials = 'user JWT and seed'; -- { serverError CANNOT_CONNECT_NATS }

-- A `SETTINGS` assignment of the key the collection already has is query-level as well,
-- so providing both sources through the `SETTINGS` clause is ambiguous in both directions.
CREATE TABLE nats_both_in_settings_over_file (key UInt64) ENGINE = NATS(04665_nats_credential_file)
SETTINGS nats_credential_file = '/var/other.creds', nats_credentials = 'user JWT and seed'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_both_in_settings_over_credentials (key UInt64) ENGINE = NATS(04665_nats_credentials)
SETTINGS nats_credentials = 'another user JWT and seed', nats_credential_file = '/var/nats.creds'; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION 04665_nats_credential_file;
DROP NAMED COLLECTION 04665_nats_credentials;
DROP NAMED COLLECTION 04665_nats_both;
