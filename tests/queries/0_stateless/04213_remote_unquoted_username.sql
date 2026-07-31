-- Tags: shard

-- An unquoted identifier is accepted as a user name in `remote` and `remoteSecure`,
-- which is consistent with how the database and table arguments work, but only when
-- it is followed by a string-literal password. This preserves the historical behavior
-- of `remote('host', db.table, sharding_key)`, where a bare identifier at the same
-- position is silently treated as a sharding key.
-- See https://github.com/ClickHouse/ClickHouse/issues/33816

-- Identifier as user name with `db.table` form, followed by a password.
SELECT * FROM remote('127.0.0.1', system.one, default, '') FORMAT Null;

-- Identifier as user name with separate database and table, followed by a password.
SELECT * FROM remote('127.0.0.1', system, one, default, '') FORMAT Null;

-- Identifier as user name, then string-literal password, then sharding key.
SELECT * FROM remote('127.0.0.1', system.one, default, '', identity(dummy)) FORMAT Null;
